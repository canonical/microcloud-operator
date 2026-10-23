#!/usr/bin/env bash
# Shared helpers for the MicroCloud charm integration tests.
#
# The integration tests exercise a real MicroCloud cluster inside LXD VMs and
# a Juju-deployed charm. They intentionally use only shell, LXD and Juju
# commands so they can be run locally and from CI.

: "${VM_IMAGE:=ubuntu-minimal-daily:resolute}"
: "${VM_CPUS:=4}"
: "${VM_MEMORY:=4GiB}"
: "${VM_ROOT_DISK:=20GiB}"
: "${CEPH_DISK_SIZE:=20GiB}"
: "${LXD_STORAGE_POOL:=default}"
: "${LXD_BRIDGE:=lxdbr0}"

: "${LXD_CHANNEL:=6/edge}"
: "${MICROCLOUD_CHANNEL:=3/edge}"
: "${MICROCEPH_CHANNEL:=tentacle/stable}"
: "${MICROOVN_CHANNEL:=26.03/edge}"

: "${COS_CHANNEL:=stable}"
: "${OTEL_CHANNEL:=0.130/stable}"
: "${OTEL_BASE:=ubuntu@26.04}"
: "${COS_OFFER_USER:=admin}"
: "${UPDATE_STATUS_INTERVAL:=10s}"

: "${UPLINK_NETWORK:=mc-uplink}"
: "${UPLINK_GATEWAY:=10.99.0.1}"
: "${UPLINK_RANGE:=10.99.0.100-10.99.0.254}"
: "${UPLINK_DNS:=8.8.8.8}"

: "${MICROCLOUD_LOG_DIR:=${TMPDIR:-/tmp}/microcloud-integration-logs}"
# Keep the SSH key outside /tmp: the Juju snap cannot read /tmp from the host.
: "${MICROCLOUD_WORK_DIR:=${RUNNER_TEMP:-${GITHUB_WORKSPACE:-$PWD}}}"

CLEANUP_MODELS=()
CLEANUP_VMS=()
CLEANUP_VOLUMES=()
CLEANUP_NETWORKS=()

log() {
    printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

die() {
    log "ERROR: $*"
    exit 1
}

run_suffix() {
    if [[ -n ${GITHUB_RUN_ID:-} ]]; then
        printf '%s-%s' "${GITHUB_RUN_ID}" "${GITHUB_RUN_ATTEMPT:-1}"
    else
        printf '%s' "$$"
    fi
}

random_token() {
    printf '%s%s' "$(date +%s)" "${RANDOM}${RANDOM}"
}

init_logging() {
    local name=$1
    mkdir -p "${MICROCLOUD_LOG_DIR}" "${MICROCLOUD_WORK_DIR}"
    WORK_DIR=$(mktemp -d "${MICROCLOUD_WORK_DIR}/microcloud-integration.XXXXXX")
    SSH_KEY="${WORK_DIR}/id_ed25519"
    ssh-keygen -q -t ed25519 -N '' -f "${SSH_KEY}"
    LOG_FILE="${MICROCLOUD_LOG_DIR}/${name}.log"
    exec > >(tee -a "${LOG_FILE}") 2>&1
    log "=== ${name} started at $(date -u +%FT%TZ) ==="
}

require_env() {
    local var
    for var in "$@"; do
        [[ -n ${!var:-} ]] || die "Required environment variable ${var} is not set"
    done
}

require_file() {
    [[ -f $1 ]] || die "Required file not found: $1"
}

retry() {
    local attempts=$1
    local delay=$2
    shift 2

    local i
    for ((i = 1; i <= attempts; i++)); do
        if "$@"; then
            return 0
        fi
        sleep "${delay}"
    done
    return 1
}

# ---------------------------------------------------------------------------
# LXD helpers
# ---------------------------------------------------------------------------

ensure_uplink_network() {
    if lxc network show "${UPLINK_NETWORK}" >/dev/null 2>&1; then
        return 0
    fi

    log "Creating isolated OVN uplink network ${UPLINK_NETWORK}"
    lxc network create "${UPLINK_NETWORK}" \
        --type=bridge \
        "ipv4.address=${UPLINK_GATEWAY}/24" \
        ipv4.dhcp=false \
        ipv6.address=none
    CLEANUP_NETWORKS+=("${UPLINK_NETWORK}")
}

vm_ip() {
    local name=$1
    local ip=""

    ip=$(lxc list "${name}" --format=json | jq -r '
        .[0].state.network.eth0.addresses[]?
        | select(.family == "inet" and .scope == "global")
        | .address
    ' 2>/dev/null | head -1 || true)

    if [[ -z ${ip} ]]; then
        ip=$(lxc exec "${name}" -- sh -c "hostname -I | awk '{print \$1}'" 2>/dev/null | tr -d '\r' | head -1 || true)
    fi

    printf '%s' "${ip}"
}

vm_hostname() {
    local name=$1
    lxc exec "${name}" -- hostname 2>/dev/null | tr -d '\r' | head -1
}

vm_primary_interface() {
    local name=$1
    local ip

    ip=$(vm_ip "${name}")
    [[ -n ${ip} ]] || die "Could not determine the primary address on ${name}"

    lxc exec "${name}" -- sh -c \
        "ip -o -4 addr show | grep -F ' ${ip}/' | awk '{print \$2; exit}'" |
        tr -d '\r'
}

vm_uplink_interface() {
    local name=$1
    local primary

    primary=$(vm_primary_interface "${name}")
    [[ -n ${primary} ]] || die "Could not determine the primary interface on ${name}"

    # shellcheck disable=SC2016 # Variables are expanded inside the VM.
    lxc exec "${name}" -- sh -c '
        primary=$1
        for path in /sys/class/net/*; do
            iface=${path##*/}
            [ "${iface}" = "lo" ] && continue
            [ "${iface}" = "${primary}" ] && continue
            printf "%s\n" "${iface}"
            exit 0
        done
        exit 1
    ' sh "${primary}"
}

prepare_uplink_interface() {
    local name=$1
    local iface

    iface=$(vm_uplink_interface "${name}")
    [[ -n ${iface} ]] || die "Could not find the OVN uplink interface on ${name}"

    log "Using ${iface} as the OVN uplink interface on ${name}"
    lxc exec "${name}" -- sh -c "
        ip -4 addr flush dev '${iface}' || true
        ip -6 addr flush dev '${iface}' || true
        ip link set '${iface}' up
    "
}

wait_vm_ready() {
    local name=$1
    local attempt
    local ip

    log "Waiting for cloud-init on ${name}"
    for ((attempt = 1; attempt <= 120; attempt++)); do
        if lxc exec "${name}" -- cloud-init status --wait >/dev/null 2>&1; then
            break
        fi
        sleep 2
    done

    lxc exec "${name}" -- cloud-init status --long >&2 || true

    for ((attempt = 1; attempt <= 120; attempt++)); do
        ip=$(vm_ip "${name}")
        if [[ -n ${ip} ]] && ssh -i "${SSH_KEY}" \
            -o BatchMode=yes \
            -o StrictHostKeyChecking=no \
            -o ConnectTimeout=5 \
            -o LogLevel=ERROR \
            "ubuntu@${ip}" true 2>/dev/null; then
            log "VM ${name} is ready at ${ip}"
            return 0
        fi
        sleep 2
    done

    die "Timed out waiting for VM ${name} to become reachable over SSH"
}

launch_vm() {
    local name=$1
    local with_ceph=$2
    local with_ovn=$3
    local user_data
    local pubkey

    pubkey=$(cat "${SSH_KEY}.pub")
    user_data=$(cat <<EOF
#cloud-config
hostname: ${name}
manage_etc_hosts: true
ssh_authorized_keys:
  - ${pubkey}
packages:
  - openssh-server
  - sudo
  - curl
  - iproute2
  - util-linux
  - rsyslog
  - snapd
package_update: true
EOF
)

    log "Launching VM ${name} (ceph=${with_ceph}, ovn=${with_ovn})"

    # Make local reruns idempotent. Names include the run suffix.
    lxc delete --force "${name}" >/dev/null 2>&1 || true
    lxc storage volume delete "${LXD_STORAGE_POOL}" "${name}-ceph" >/dev/null 2>&1 || true

    lxc init "${VM_IMAGE}" "${name}" --vm \
        --config "limits.cpu=${VM_CPUS}" \
        --config "limits.memory=${VM_MEMORY}" \
        --config "cloud-init.user-data=${user_data}"
    CLEANUP_VMS+=("${name}")

    if ! lxc config device override "${name}" root size="${VM_ROOT_DISK}" >/dev/null 2>&1; then
        log "WARNING: could not override the root disk size for ${name}"
    fi

    if [[ ${with_ovn} == yes ]]; then
        ensure_uplink_network
        lxc config device add "${name}" eth1 nic network="${UPLINK_NETWORK}" name=eth1
    fi

    if [[ ${with_ceph} == yes ]]; then
        lxc storage volume create "${LXD_STORAGE_POOL}" "${name}-ceph" \
            --type=block size="${CEPH_DISK_SIZE}"
        CLEANUP_VOLUMES+=("${name}-ceph")
        lxc config device add "${name}" ceph disk \
            pool="${LXD_STORAGE_POOL}" source="${name}-ceph"
    fi

    lxc start "${name}"
    wait_vm_ready "${name}"

    if [[ ${with_ovn} == yes ]]; then
        prepare_uplink_interface "${name}"
    fi
}

vm_disk() {
    local name=$1
    # Select the largest writable, unmounted disk. This skips the root disk
    # (its partitions are mounted) and LXD's small cloud-init config drive.
    # shellcheck disable=SC2016 # Variables are expanded inside the VM.
    lxc exec "${name}" -- sh -c '
        for disk in /dev/sd? /dev/vd? /dev/nvme?n?; do
            [ -b "${disk}" ] || continue
            lsblk -no MOUNTPOINT "${disk}" | grep -q "[^[:space:]]" && continue
            [ "$(lsblk -dno RO "${disk}")" = "0" ] || continue
            printf "%s %s\n" "$(lsblk -bdno SIZE "${disk}")" "${disk}"
        done | sort -nr | head -1 | cut -d" " -f2
    '
}

snap_install_or_refresh() {
    local name=$1
    local snap=$2
    local channel=$3

    if lxc exec "${name}" -- snap list "${snap}" >/dev/null 2>&1; then
        log "Refreshing ${snap} on ${name} from ${channel}"
        lxc exec "${name}" -- snap refresh "${snap}" --channel="${channel}"
    else
        log "Installing ${snap} on ${name} from ${channel}"
        lxc exec "${name}" -- snap install "${snap}" --channel="${channel}"
    fi
}

install_microcloud_snaps() {
    local name=$1
    local with_ceph=$2
    local with_ovn=$3
    local hold_snaps=(lxd microcloud)

    snap_install_or_refresh "${name}" lxd "${LXD_CHANNEL}"

    if [[ ${with_ceph} == yes ]]; then
        snap_install_or_refresh "${name}" microceph "${MICROCEPH_CHANNEL}"
        hold_snaps+=(microceph)
    fi

    if [[ ${with_ovn} == yes ]]; then
        snap_install_or_refresh "${name}" microovn "${MICROOVN_CHANNEL}"
        hold_snaps+=(microovn)
    fi

    snap_install_or_refresh "${name}" microcloud "${MICROCLOUD_CHANNEL}"
    lxc exec "${name}" -- snap refresh --hold "${hold_snaps[@]}"
}

wait_for_microcloud_ready() {
    local name=$1
    local attempt

    for ((attempt = 1; attempt <= 120; attempt++)); do
        if lxc exec "${name}" -- microcloud cluster list >/dev/null 2>&1; then
            log "MicroCloud is ready on ${name}"
            return 0
        fi
        sleep 5
    done

    lxc exec "${name}" -- microcloud cluster list || true
    die "Timed out waiting for MicroCloud on ${name}"
}

preseed_common_header() {
    local initiator=$1
    local passphrase=$2

    printf 'lookup_subnet: %s\n' "$(lxc network get "${LXD_BRIDGE}" ipv4.address)"
    printf 'initiator: %s\n' "${initiator}"
    printf 'session_passphrase: %s\n' "${passphrase}"
}

preseed_system() {
    local name=$1
    local with_ceph=$2
    local with_ovn=$3
    local indent=$4

    printf '%s- name: %s\n' "${indent}" "${name}"
    if [[ ${with_ovn} == yes ]]; then
        local uplink
        uplink=$(vm_uplink_interface "${name}")
        [[ -n ${uplink} ]] || die "Could not find the OVN uplink interface on ${name}"
        printf '%s  ovn_uplink_interface: %s\n' "${indent}" "${uplink}"
    fi
    if [[ ${with_ceph} == yes ]]; then
        local disk
        disk=$(vm_disk "${name}")
        [[ -n ${disk} ]] || die "Could not find the Ceph disk on ${name}"
        printf '%s  storage:\n' "${indent}"
        printf '%s    ceph:\n' "${indent}"
        printf '%s    - path: %s\n' "${indent}" "${disk}"
        printf '%s      wipe: true\n' "${indent}"
    fi
}

preseed_global_config() {
    local with_ceph=$1
    local with_ovn=$2

    if [[ ${with_ceph} == yes ]]; then
        printf 'ceph:\n'
        printf '  cephfs: false\n'
    fi

    if [[ ${with_ovn} == yes ]]; then
        printf 'ovn:\n'
        printf '  ipv4_gateway: %s/24\n' "${UPLINK_GATEWAY}"
        printf '  ipv4_range: %s\n' "${UPLINK_RANGE}"
        printf '  dns_servers: %s\n' "${UPLINK_DNS}"
    fi
}

run_preseed() {
    local name=$1
    local preseed_file=$2
    local log_file=$3

    lxc file push "${preseed_file}" "${name}/tmp/microcloud-preseed.yaml"
    if ! lxc exec "${name}" -- env TEST_CONSOLE=0 \
        sh -c "microcloud preseed < /tmp/microcloud-preseed.yaml > ${log_file} 2>&1"; then
        lxc exec "${name}" -- cat "${log_file}" || true
        die "microcloud preseed failed on ${name}"
    fi
    lxc exec "${name}" -- cat "${log_file}" || true
}

preseed_init() {
    local name=$1
    local with_ceph=$2
    local with_ovn=$3
    local host
    local preseed_file="${WORK_DIR}/preseed-init.yaml"

    host=$(vm_hostname "${name}")
    [[ -n ${host} ]] || die "Could not determine hostname for ${name}"

    {
        preseed_common_header "${host}" "$(random_token)"
        printf 'systems:\n'
        preseed_system "${host}" "${with_ceph}" "${with_ovn}" ""
        preseed_global_config "${with_ceph}" "${with_ovn}"
    } >"${preseed_file}"

    log "Initialising MicroCloud on ${name}"
    run_preseed "${name}" "${preseed_file}" /tmp/microcloud-preseed.out

    lxc exec "${name}" -- grep -q "MicroCloud is ready" /tmp/microcloud-preseed.out ||
        die "MicroCloud did not initialise on ${name}"
    wait_for_microcloud_ready "${name}"
}

preseed_add_member() {
    local initiator=$1
    local member=$2
    local with_ceph=$3
    local with_ovn=$4
    local initiator_host
    local member_host
    local passphrase
    local preseed_file="${WORK_DIR}/preseed-add-member.yaml"
    local member_pid
    local initiator_rc=0
    local member_rc=0

    initiator_host=$(vm_hostname "${initiator}")
    member_host=$(vm_hostname "${member}")
    passphrase=$(random_token)

    {
        preseed_common_header "${initiator_host}" "${passphrase}"
        printf 'systems:\n'
        preseed_system "${member_host}" "${with_ceph}" "${with_ovn}" ""
        preseed_global_config "${with_ceph}" "${with_ovn}"
    } >"${preseed_file}"

    lxc file push "${preseed_file}" "${initiator}/tmp/microcloud-preseed.yaml"
    lxc file push "${preseed_file}" "${member}/tmp/microcloud-preseed.yaml"

    log "Adding ${member} to the MicroCloud cluster on ${initiator}"
    lxc exec "${member}" -- env TEST_CONSOLE=0 \
        sh -c 'microcloud preseed < /tmp/microcloud-preseed.yaml > /tmp/microcloud-preseed.out 2>&1' &
    member_pid=$!

    set +e
    lxc exec "${initiator}" -- env TEST_CONSOLE=0 \
        sh -c 'microcloud preseed < /tmp/microcloud-preseed.yaml > /tmp/microcloud-preseed.out 2>&1'
    initiator_rc=$?
    wait "${member_pid}"
    member_rc=$?
    set -e

    lxc exec "${initiator}" -- cat /tmp/microcloud-preseed.out || true
    lxc exec "${member}" -- cat /tmp/microcloud-preseed.out || true

    if ((initiator_rc != 0 || member_rc != 0)); then
        die "microcloud preseed failed while adding ${member}"
    fi
    lxc exec "${initiator}" -- grep -q "MicroCloud is ready" /tmp/microcloud-preseed.out ||
        die "The initiator did not report a ready MicroCloud"
    lxc exec "${member}" -- grep -q "Successfully joined the MicroCloud cluster" /tmp/microcloud-preseed.out ||
        die "${member} did not join the MicroCloud cluster"
    wait_for_microcloud_ready "${initiator}"

    local attempt
    for ((attempt = 1; attempt <= 60; attempt++)); do
        if lxc exec "${initiator}" -- microcloud cluster list | grep -q "${member_host}"; then
            log "${member_host} is now part of the MicroCloud cluster"
            return 0
        fi
        sleep 5
    done
    lxc exec "${initiator}" -- microcloud cluster list || true
    die "Timed out waiting for ${member_host} to appear in the MicroCloud cluster"
}

# ---------------------------------------------------------------------------
# Juju helpers
# ---------------------------------------------------------------------------

juju_target() {
    printf '%s:%s' "$1" "$2"
}

create_model() {
    local controller=$1
    local model=$2

    log "Creating Juju model ${controller}:${model}"
    juju add-model --controller "${controller}" "${model}"
    CLEANUP_MODELS+=("${controller}|${model}")
}

juju_machine_id_by_ip() {
    local target=$1
    local ip=$2
    local machine_id

    machine_id=$(juju status -m "${target}" --format=json 2>/dev/null | jq -r --arg ip "${ip}" '
        .machines | to_entries[]
        | select((.value["ip-addresses"] // []) | index($ip))
        | .key
    ' | head -1 || true)

    if [[ -z ${machine_id} ]]; then
        machine_id=$(juju status -m "${target}" --format=json 2>/dev/null |
            jq -r '.machines | keys[]' | sort -n | tail -1 || true)
    fi

    printf '%s' "${machine_id}"
}

juju_add_machine_diagnostics() {
    local ip=$1

    log "Collecting VM diagnostics for ${ip}"
    ssh -i "${SSH_KEY}" \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=5 \
        -o LogLevel=ERROR \
        "ubuntu@${ip}" '
            echo "--- systemctl status rsyslog ---"
            sudo systemctl status rsyslog --no-pager
            echo "--- cloud-init status ---"
            sudo cloud-init status --long
            echo "--- journalctl -u rsyslog ---"
            sudo journalctl -u rsyslog --no-pager -n 100
            echo "--- cloud-init-output.log ---"
            sudo tail -n 200 /var/log/cloud-init-output.log
        ' || log "WARNING: could not collect diagnostics from ${ip}"
}

juju_add_machine() {
    local controller=$1
    local model=$2
    local ip=$3
    local target
    local output
    local machine_id
    local rc=0

    target=$(juju_target "${controller}" "${model}")
    log "Adding ${ip} as a Juju machine in ${target}"

    # Record the host key in known_hosts before Juju connects.
    retry 12 5 ssh -i "${SSH_KEY}" \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=5 \
        -o LogLevel=ERROR \
        "ubuntu@${ip}" true ||
        die "Could not SSH to ${ip} before adding it as a Juju machine"

    set +e
    output=$(timeout 1200 juju add-machine -m "${target}" "ssh:ubuntu@${ip}" \
        --private-key "${SSH_KEY}" \
        --public-key "${SSH_KEY}.pub" </dev/null 2>&1)
    rc=$?
    set -e
    log "${output}"

    if ((rc != 0)); then
        juju_status_dump "${target}"
        juju_add_machine_diagnostics "${ip}"
        die "juju add-machine failed for ${ip} (exit code ${rc})"
    fi

    machine_id=$(printf '%s\n' "${output}" |
        sed -n 's/.*created machine \([0-9][0-9]*\).*/\1/p' | head -1)
    if [[ -z ${machine_id} ]]; then
        machine_id=$(juju_machine_id_by_ip "${target}" "${ip}")
    fi
    [[ -n ${machine_id} ]] || die "Could not determine the Juju machine ID for ${ip}"

    printf '%s' "${machine_id}"
}

juju_status_dump() {
    local target=$1
    log "Juju status for ${target}:"
    juju status -m "${target}" --relations || true
    log "Recent Juju debug log for ${target}:"
    juju debug-log -m "${target}" --limit=200 || true
}

unit_status() {
    local target=$1
    local app=$2
    local unit=$3

    juju status -m "${target}" --format=json 2>/dev/null | jq -r \
        --arg app "${app}" \
        --arg unit "${unit}" \
        '(.applications[$app].units[$unit]
          // ([.applications[]?.units[]? | .subordinates[$unit]?] | first))
         ["workload-status"].current // empty' 2>/dev/null || true
}

unit_message() {
    local target=$1
    local app=$2
    local unit=$3

    juju status -m "${target}" --format=json 2>/dev/null | jq -r \
        --arg app "${app}" \
        --arg unit "${unit}" \
        '(.applications[$app].units[$unit]
          // ([.applications[]?.units[]? | .subordinates[$unit]?] | first))
         ["workload-status"].message // empty' 2>/dev/null || true
}

wait_for_unit_status() {
    local target=$1
    local app=$2
    local unit=$3
    local want=$4
    local timeout=${5:-1800}
    local deadline=$((SECONDS + timeout))
    local current

    while ((SECONDS < deadline)); do
        current=$(unit_status "${target}" "${app}" "${unit}")
        if [[ ${current} == "${want}" ]]; then
            return 0
        fi
        if [[ ${current} == error ]]; then
            juju_status_dump "${target}"
            die "Unit ${unit} entered error state"
        fi
        sleep 5
    done

    juju_status_dump "${target}"
    die "Timed out waiting for ${unit} to become ${want}"
}

wait_for_unit_message() {
    local target=$1
    local app=$2
    local unit=$3
    local want_status=$4
    local want_message=$5
    local timeout=${6:-1800}
    local deadline=$((SECONDS + timeout))
    local current
    local message

    while ((SECONDS < deadline)); do
        current=$(unit_status "${target}" "${app}" "${unit}")
        message=$(unit_message "${target}" "${app}" "${unit}")
        if [[ ${current} == "${want_status}" && ${message} == *"${want_message}"* ]]; then
            return 0
        fi
        if [[ ${current} == error ]]; then
            juju_status_dump "${target}"
            die "Unit ${unit} entered error state: ${message}"
        fi
        sleep 5
    done

    juju_status_dump "${target}"
    die "Timed out waiting for ${unit} to become ${want_status} with message containing '${want_message}'"
}

wait_for_apps_active() {
    local target=$1
    local timeout=$2
    shift 2

    local apps_json
    local deadline=$((SECONDS + timeout))

    apps_json=$(printf '%s\n' "$@" | jq -R . | jq -s .)

    while ((SECONDS < deadline)); do
        if juju status -m "${target}" --format=json 2>/dev/null | jq -e \
            --argjson apps "${apps_json}" '
            (.applications // {}) as $applications
            | ($apps | all(.[]; . as $name | (($applications[$name].units // {}) | length) > 0))
            and
            ([$apps[] as $name | $applications[$name].units[]? | ."workload-status".current]
                | all(. == "active"))
        ' >/dev/null 2>&1; then
            return 0
        fi
        sleep 5
    done

    juju_status_dump "${target}"
    die "Timed out waiting for $* in ${target} to become active"
}

juju_run_results() {
    local target=$1
    local unit=$2
    local action=$3

    juju run -m "${target}" "${unit}" "${action}" --format=json |
        jq -r 'to_entries[0].value.results'
}

juju_ssh() {
    local target=$1
    local unit=$2
    shift 2
    juju ssh -m "${target}" "${unit}" -- "$@"
}

assert_status_action() {
    local target=$1
    local unit=$2
    local expected_count=$3
    shift 3
    local results
    local initialized
    local consistent
    local members
    local count
    local name

    results=$(juju_run_results "${target}" "${unit}" status)
    initialized=$(jq -r '.initialized' <<<"${results}")
    consistent=$(jq -r '.consistent' <<<"${results}")
    members=$(jq -r '.members' <<<"${results}")

    [[ ${initialized,,} == true ]] || die "MicroCloud is not initialised according to ${unit}"
    [[ ${consistent,,} == true ]] || die "MicroCloud membership is inconsistent according to ${unit}"

    count=$(jq 'length' <<<"${members}")
    [[ ${count} -eq ${expected_count} ]] ||
        die "Expected ${expected_count} MicroCloud members, got ${count} according to ${unit}"

    for name in "$@"; do
        jq -e --arg name "${name}" 'map(.name) | index($name) != null' <<<"${members}" >/dev/null ||
            die "MicroCloud member ${name} is missing according to ${unit}"
    done
}

scrape_configs() {
    local target=$1
    local unit=$2
    juju_run_results "${target}" "${unit}" dump-metrics-config |
        jq -r '.["scrape-configs"]'
}

assert_local_metrics() {
    local target=$1
    local unit=$2
    local url=$3

    retry 60 5 juju_ssh "${target}" "${unit}" -- sh -c "curl -ksf '${url}' >/dev/null" ||
        die "Metrics endpoint ${url} is not reachable from ${unit}"
}

assert_ovn_exporter_active() {
    local target=$1
    local unit=$2

    retry 60 5 juju_ssh "${target}" "${unit}" -- sh -c \
        'snap services ovn-exporter.ovn-exporter | grep -Eq "[[:space:]]active[[:space:]]"' ||
        die "ovn-exporter is not active on ${unit}"
}

assert_no_ovn_exporter() {
    local target=$1
    local unit=$2

    if juju_ssh "${target}" "${unit}" -- sh -c 'snap list ovn-exporter >/dev/null 2>&1'; then
        die "ovn-exporter is installed on ${unit} but MicroOVN is absent"
    fi
}

# ---------------------------------------------------------------------------
# COS helpers
# ---------------------------------------------------------------------------

setup_cos() {
    local model
    local target
    local traefik_status

    model="cos-$(run_suffix)"

    require_env K8S_CONTROLLER
    target=$(juju_target "${K8S_CONTROLLER}" "${model}")

    # Used by run.sh's EXIT trap, even though this library does not read it.
    export CLEANUP_COS_MODEL="${model}"
    log "Deploying COS Lite (${COS_CHANNEL}) in ${target}"
    juju add-model --controller "${K8S_CONTROLLER}" "${model}"
    juju deploy -m "${target}" cos-lite --channel="${COS_CHANNEL}" --trust

    # Traefik/ingress is not used by these tests, and MetalLB is not always
    # able to allocate an address on CI runners. Wait for the observability
    # backends only.
    wait_for_apps_active "${target}" 1800 \
        prometheus loki grafana alertmanager catalogue

    traefik_status=$(unit_status "${target}" traefik traefik/0)
    if [[ ${traefik_status} != active ]]; then
        log "WARNING: traefik is ${traefik_status:-unknown}; continuing because COS ingress is not used"
    fi

    log "Offering COS Lite endpoints from ${target}"
    juju offer --controller "${K8S_CONTROLLER}" \
        "${model}.prometheus:receive-remote-write"
    juju offer --controller "${K8S_CONTROLLER}" \
        "${model}.loki:logging"
    juju offer --controller "${K8S_CONTROLLER}" \
        "${model}.grafana:grafana-dashboard"
}

teardown_cos() {
    local model=$1
    local target

    [[ -n ${model} ]] || return 0
    target=$(juju_target "${K8S_CONTROLLER}" "${model}")

    log "Destroying COS model ${target}"
    juju destroy-model "${target}" --no-prompt --destroy-storage --force ||
        log "WARNING: failed to destroy COS model ${target}"
}

consume_cos() {
    local target=$1
    local cos_model=$2
    local offer_prefix="${K8S_CONTROLLER}:${COS_OFFER_USER}/${cos_model}"
    local app

    for app in prometheus loki grafana; do
        log "Consuming ${offer_prefix}.${app}"
        juju consume -m "${target}" "${offer_prefix}.${app}"
    done
}

deploy_and_relate_cos() {
    local target=$1
    local cos_model=$2

    consume_cos "${target}" "${cos_model}"

    log "Deploying opentelemetry-collector (${OTEL_CHANNEL}, ${OTEL_BASE})"
    juju deploy -m "${target}" opentelemetry-collector \
        --channel="${OTEL_CHANNEL}" \
        --base="${OTEL_BASE}"

    log "Relating the MicroCloud charm to the COS collector"
    juju relate -m "${target}" opentelemetry-collector microcloud:cos-agent
    juju relate -m "${target}" opentelemetry-collector microcloud:logging
    juju relate -m "${target}" opentelemetry-collector prometheus
    juju relate -m "${target}" opentelemetry-collector loki
    juju relate -m "${target}" opentelemetry-collector grafana
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

cleanup_all() {
    local exit_code=$?
    local entry
    local controller
    local model
    local vm
    local volume
    local network

    set +e

    if ((${#CLEANUP_MODELS[@]})); then
        for entry in "${CLEANUP_MODELS[@]}"; do
            controller=${entry%%|*}
            model=${entry#*|}
            log "Destroying Juju model ${controller}:${model}"
            juju destroy-model "${controller}:${model}" \
                --no-prompt --destroy-storage --force ||
                log "WARNING: failed to destroy ${controller}:${model}"
        done
    fi

    if ((${#CLEANUP_VMS[@]})); then
        for vm in "${CLEANUP_VMS[@]}"; do
            log "Deleting VM ${vm}"
            lxc delete --force "${vm}" || log "WARNING: failed to delete ${vm}"
        done
    fi

    if ((${#CLEANUP_VOLUMES[@]})); then
        for volume in "${CLEANUP_VOLUMES[@]}"; do
            log "Deleting storage volume ${volume}"
            lxc storage volume delete "${LXD_STORAGE_POOL}" "${volume}" ||
                log "WARNING: failed to delete ${volume}"
        done
    fi

    if ((${#CLEANUP_NETWORKS[@]})); then
        for network in "${CLEANUP_NETWORKS[@]}"; do
            log "Deleting network ${network}"
            lxc network delete "${network}" || log "WARNING: failed to delete ${network}"
        done
    fi

    if [[ -n ${WORK_DIR:-} ]]; then
        rm -rf "${WORK_DIR}"
    fi

    exit "${exit_code}"
}
