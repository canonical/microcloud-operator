#!/usr/bin/env bash
# Deploy the charm before MicroCloud exists, initialise MicroCloud, then grow
# the cluster with another member and a second charm unit.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

init_logging test_grow
trap cleanup_all EXIT

main() {
    require_env LXD_CONTROLLER MICROCLOUD_CHARM_PATH

    local suffix
    local vm1
    local vm2
    local model
    local target
    local ip1
    local ip2
    local machine1
    local machine2
    local host1
    local host2

    suffix=$(run_suffix)
    vm1="mc-grow-a-${suffix}"
    vm2="mc-grow-b-${suffix}"
    model="mc-grow-${suffix}"
    target=$(juju_target "${LXD_CONTROLLER}" "${model}")

    launch_vm "${vm1}" yes yes
    host1=$(vm_hostname "${vm1}")

    create_model "${LXD_CONTROLLER}" "${model}"
    juju model-config -m "${target}" "update-status-hook-interval=${UPDATE_STATUS_INTERVAL}"

    ip1=$(vm_ip "${vm1}")
    machine1=$(juju_add_machine "${LXD_CONTROLLER}" "${model}" "${ip1}")

    log "Deploying the charm before MicroCloud is formed"
    juju deploy -m "${target}" "${MICROCLOUD_CHARM_PATH}" --to "${machine1}"
    wait_for_unit_message "${target}" microcloud microcloud/0 blocked \
        "MicroCloud is not initialized on this node" 1800

    log "Forming a single-node MicroCloud beneath the existing charm"
    install_microcloud_snaps "${vm1}" yes yes
    preseed_init "${vm1}" yes yes
    wait_for_unit_message "${target}" microcloud microcloud/0 active \
        "Waiting for observability relation" 1800
    assert_status_action "${target}" microcloud/0 1 "${host1}"

    log "Adding a second MicroCloud member"
    launch_vm "${vm2}" yes yes
    host2=$(vm_hostname "${vm2}")
    install_microcloud_snaps "${vm2}" yes yes
    preseed_add_member "${vm1}" "${vm2}" yes yes

    ip2=$(vm_ip "${vm2}")
    machine2=$(juju_add_machine "${LXD_CONTROLLER}" "${model}" "${ip2}")

    log "Adding a second charm unit on the new member"
    juju add-unit -m "${target}" microcloud --to "${machine2}"
    wait_for_unit_message "${target}" microcloud microcloud/1 active \
        "Waiting for observability relation" 1800

    log "Checking that both charm units see both MicroCloud members"
    assert_status_action "${target}" microcloud/0 2 "${host1}" "${host2}"
    assert_status_action "${target}" microcloud/1 2 "${host1}" "${host2}"

    lxc exec "${vm1}" -- microcloud cluster list | grep -q "${host1}" ||
        die "${host1} is missing from the MicroCloud cluster list"
    lxc exec "${vm1}" -- microcloud cluster list | grep -q "${host2}" ||
        die "${host2} is missing from the MicroCloud cluster list"
    lxc exec "${vm2}" -- microcloud cluster list | grep -q "${host1}" ||
        die "${host1} is missing from the MicroCloud cluster list on ${host2}"

    log "Grow integration scenario passed"
}

main "$@"
