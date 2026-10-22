#!/usr/bin/env bash
# MicroCloud without MicroCeph and MicroOVN; the charm must adapt.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

init_logging test_without_ceph_ovn
trap cleanup_all EXIT

main() {
    require_env LXD_CONTROLLER K8S_CONTROLLER COS_MODEL MICROCLOUD_CHARM_PATH

    local suffix
    local vm
    local model
    local target
    local ip
    local machine
    local host
    local configs

    suffix=$(run_suffix)
    vm="mc-min-${suffix}"
    model="mc-min-${suffix}"
    target=$(juju_target "${LXD_CONTROLLER}" "${model}")

    launch_vm "${vm}" no no
    install_microcloud_snaps "${vm}" no no
    preseed_init "${vm}" no no
    host=$(vm_hostname "${vm}")

    create_model "${LXD_CONTROLLER}" "${model}"
    ip=$(vm_ip "${vm}")
    machine=$(juju_add_machine "${LXD_CONTROLLER}" "${model}" "${ip}")

    log "Deploying the packed charm ${MICROCLOUD_CHARM_PATH}"
    juju deploy -m "${target}" "${MICROCLOUD_CHARM_PATH}" --to "${machine}"
    wait_for_unit_message "${target}" microcloud microcloud/0 active \
        "Waiting for observability relation" 1800

    deploy_and_relate_cos "${target}" "${COS_MODEL}"
    wait_for_unit_status "${target}" opentelemetry-collector \
        opentelemetry-collector/0 active 1800
    wait_for_unit_message "${target}" microcloud microcloud/0 active \
        "Observability active" 1800

    log "Checking MicroCloud membership reported by the charm"
    assert_status_action "${target}" microcloud/0 1 "${host}"

    log "Checking that only the LXD scrape job is advertised"
    configs=$(scrape_configs "${target}" microcloud/0)
    jq -e 'length == 1' <<<"${configs}" >/dev/null ||
        die "Expected exactly one scrape job without MicroCeph and MicroOVN"
    jq -e '.[0].job_name | startswith("microcloud-lxd-")' <<<"${configs}" >/dev/null ||
        die "The only scrape job should be the LXD job"

    log "Checking that the LXD metrics endpoint is reachable"
    assert_local_metrics "${target}" microcloud/0 "https://127.0.0.1:8444/1.0/metrics"
    assert_no_ovn_exporter "${target}" microcloud/0

    log "No-Ceph/OVN integration scenario passed"
}

main "$@"
