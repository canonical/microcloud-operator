#!/usr/bin/env bash
# COS Lite + single-node MicroCloud (LXD, MicroCeph, MicroOVN) + charm.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

init_logging test_cos
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
    vm="mc-cos-${suffix}"
    model="mc-cos-${suffix}"
    target=$(juju_target "${LXD_CONTROLLER}" "${model}")

    launch_vm "${vm}" yes yes
    install_microcloud_snaps "${vm}" yes yes
    preseed_init "${vm}" yes yes
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

    log "Checking scrape configuration"
    configs=$(scrape_configs "${target}" microcloud/0)
    jq -e 'map(.job_name) | any(startswith("microcloud-lxd-"))' <<<"${configs}" >/dev/null ||
        die "The LXD scrape job is missing"
    jq -e 'map(.job_name) | any(startswith("microcloud-microceph-"))' <<<"${configs}" >/dev/null ||
        die "The MicroCeph scrape job is missing"
    jq -e 'map(.job_name) | any(startswith("microcloud-microovn-"))' <<<"${configs}" >/dev/null ||
        die "The MicroOVN scrape job is missing"

    log "Checking local metrics endpoints"
    assert_local_metrics "${target}" microcloud/0 "https://127.0.0.1:8444/1.0/metrics"
    assert_local_metrics "${target}" microcloud/0 "http://127.0.0.1:9283/metrics"
    assert_local_metrics "${target}" microcloud/0 "http://127.0.0.1:9310/metrics"
    assert_ovn_exporter_active "${target}" microcloud/0

    log "COS integration scenario passed"
}

main "$@"
