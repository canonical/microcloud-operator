#!/usr/bin/env bash
# Entrypoint for the MicroCloud charm integration tests.
#
# Required environment:
#   MICROCLOUD_CHARM_PATH  Path to the packed .charm under test
#   LXD_CONTROLLER         Juju controller backed by LXD
#   K8S_CONTROLLER         Juju controller backed by MicroK8s
#
# Optional environment: see lib.sh for image, VM size, snap and COS channels.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

CLEANUP_COS_MODEL=""

cleanup_cos_on_exit() {
    local exit_code=$?
    set +e
    if [[ -n ${CLEANUP_COS_MODEL} ]]; then
        teardown_cos "${CLEANUP_COS_MODEL}"
    fi
    if [[ -n ${WORK_DIR:-} ]]; then
        rm -rf "${WORK_DIR}"
    fi
    exit "${exit_code}"
}

main() {
    init_logging run
    trap cleanup_cos_on_exit EXIT

    require_env MICROCLOUD_CHARM_PATH LXD_CONTROLLER K8S_CONTROLLER
    require_file "${MICROCLOUD_CHARM_PATH}"
    export MICROCLOUD_CHARM_PATH LXD_CONTROLLER K8S_CONTROLLER

    setup_cos
    export COS_MODEL="${CLEANUP_COS_MODEL}"

    log "Running COS-backed integration scenarios"
    bash "${SCRIPT_DIR}/test_cos.sh"
    bash "${SCRIPT_DIR}/test_without_ceph_ovn.sh"

    log "Tearing down COS before the grow scenario"
    teardown_cos "${COS_MODEL}"
    CLEANUP_COS_MODEL=""
    unset COS_MODEL

    bash "${SCRIPT_DIR}/test_grow.sh"

    log "All integration scenarios passed"
}

main "$@"
