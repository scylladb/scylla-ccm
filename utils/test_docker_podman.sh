#!/bin/bash
#
# Test script for PR #722: Docker/Podman container client abstraction
# Usage:
#   ./test_docker_podman.sh              # Run all tests (docker + podman)
#   ./test_docker_podman.sh docker       # Docker tests only
#   ./test_docker_podman.sh podman       # Podman tests only
#   ./test_docker_podman.sh unit         # Unit tests only (no runtime needed)
#   ./test_docker_podman.sh pytest       # Run pytest integration tests
#
# Environment variables:
#   SCYLLA_DOCKER_IMAGE  - Docker image to use (default: scylladb/scylla-nightly:latest)
#   CCM_CMD              - CCM command (default: ./ccm)
#   SKIP_CLEANUP         - Set to 1 to skip cleanup (for debugging)

set -euo pipefail

# -- Configuration --
SCYLLA_DOCKER_IMAGE="${SCYLLA_DOCKER_IMAGE:-scylladb/scylla-nightly:latest}"
CCM_CMD="${CCM_CMD:-./ccm}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; PASS_COUNT=$((PASS_COUNT + 1)); }
log_fail()  { echo -e "${RED}[FAIL]${NC}  $*"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
log_skip()  { echo -e "${YELLOW}[SKIP]${NC}  $*"; SKIP_COUNT=$((SKIP_COUNT + 1)); }
log_section() { echo -e "\n${BLUE}━━━ $* ━━━${NC}"; }

cleanup_cluster() {
    local name="$1"
    if [[ "$SKIP_CLEANUP" == "1" ]]; then
        log_info "SKIP_CLEANUP=1, leaving cluster '$name' intact"
        return 0
    fi
    log_info "Cleaning up cluster '$name'..."
    $CCM_CMD switch "$name" 2>/dev/null && $CCM_CMD remove 2>/dev/null || true
}

# cleanup any leftover clusters from a previous run
cleanup_all() {
    for name in docker-test-1n docker-test-3n podman-test-1n podman-test-3n env-docker-test env-podman-test; do
        cleanup_cluster "$name" 2>/dev/null || true
    done
}

# ===================================================================
# Phase 0: Pre-flight checks
# ===================================================================
preflight() {
    log_section "Phase 0: Pre-flight checks"

    # Check we're on the right branch
    local branch
    branch=$(git branch --show-current 2>/dev/null || echo "unknown")
    log_info "Current branch: $branch"

    if ! $CCM_CMD list >/dev/null 2>&1; then
        log_fail "CCM command '$CCM_CMD' not runnable. Run 'uv sync' first."
        exit 1
    fi
    log_pass "CCM command works"

    # Check container_client.py exists (PR #722 file)
    if [[ ! -f ccmlib/container_client.py ]]; then
        log_fail "ccmlib/container_client.py not found. Are you on the PR #722 branch?"
        exit 1
    fi
    log_pass "container_client.py exists"

    # Check runtimes
    if command -v docker &>/dev/null; then
        log_pass "Docker available: $(docker --version)"
    else
        log_skip "Docker not available"
    fi

    if command -v podman &>/dev/null; then
        log_pass "Podman available: $(podman --version)"
    else
        log_skip "Podman not available"
    fi

    log_info "Using image: $SCYLLA_DOCKER_IMAGE"
}

# ===================================================================
# Phase 1: Unit tests (no container runtime needed)
# ===================================================================
run_unit_tests() {
    log_section "Phase 1: Unit tests (mocked, no runtime needed)"

    if uv run python -m pytest tests/test_container_client.py -v --tb=short 2>&1; then
        log_pass "Container client unit tests passed"
    else
        log_fail "Container client unit tests failed"
    fi
}

# ===================================================================
# Phase 2: Docker CLI tests
# ===================================================================
run_docker_tests() {
    log_section "Phase 2: Docker runtime tests"

    if ! command -v docker &>/dev/null; then
        log_skip "Docker not installed, skipping Docker tests"
        return
    fi

    if ! docker info &>/dev/null 2>&1; then
        log_skip "Docker daemon not running, skipping Docker tests"
        return
    fi

    # 2a: Pull image
    log_info "Pulling image: $SCYLLA_DOCKER_IMAGE"
    if docker pull "$SCYLLA_DOCKER_IMAGE"; then
        log_pass "Docker image pulled: $SCYLLA_DOCKER_IMAGE"
    else
        log_fail "Failed to pull Docker image"
        return
    fi

    # 2b: 1-node cluster create + start + status + stop + remove
    log_info "Test: 1-node Docker cluster lifecycle"
    if $CCM_CMD create docker-test-1n --scylla -n 1 \
        --docker-image "$SCYLLA_DOCKER_IMAGE" -s 2>&1; then
        log_pass "Docker 1-node cluster created and started"
    else
        log_fail "Docker 1-node cluster create failed"
        cleanup_cluster docker-test-1n
        return
    fi

    # Check status
    if $CCM_CMD status 2>&1 | grep -q "UP"; then
        log_pass "Docker 1-node cluster status: node is UP"
    else
        log_fail "Docker 1-node cluster status: node not UP"
        $CCM_CMD status 2>&1 || true
    fi

    # Test cqlsh
    if $CCM_CMD node1 cqlsh -e "DESCRIBE KEYSPACES;" 2>&1; then
        log_pass "Docker 1-node cqlsh works"
    else
        log_fail "Docker 1-node cqlsh failed"
    fi

    # Test nodetool
    if $CCM_CMD node1 nodetool status 2>&1 | grep -q "UN"; then
        log_pass "Docker 1-node nodetool status: UN"
    else
        log_fail "Docker 1-node nodetool status failed"
    fi

    # Stop + remove
    $CCM_CMD stop 2>&1 || true
    cleanup_cluster docker-test-1n

    # 2c: 3-node cluster
    log_info "Test: 3-node Docker cluster"
    if $CCM_CMD create docker-test-3n --scylla -n 3 \
        --docker-image "$SCYLLA_DOCKER_IMAGE" -s 2>&1; then
        log_pass "Docker 3-node cluster created and started"
    else
        log_fail "Docker 3-node cluster create failed"
        cleanup_cluster docker-test-3n
        return
    fi

    if $CCM_CMD status 2>&1 | grep -c "UP" | grep -q "3"; then
        log_pass "Docker 3-node cluster: all 3 nodes UP"
    else
        log_fail "Docker 3-node cluster: not all nodes UP"
        $CCM_CMD status 2>&1 || true
    fi

    # Test nodetool sees all 3 nodes
    local un_count
    un_count=$($CCM_CMD node1 nodetool status 2>&1 | grep -c "^UN" || echo "0")
    if [[ "$un_count" -eq 3 ]]; then
        log_pass "Docker 3-node nodetool: all 3 nodes UN"
    else
        log_fail "Docker 3-node nodetool: expected 3 UN nodes, got $un_count"
        $CCM_CMD node1 nodetool status 2>&1 || true
    fi

    # Test CQL write + read across nodes
    log_info "Test: CQL write/read on 3-node Docker cluster"
    $CCM_CMD node1 cqlsh -e "
        CREATE KEYSPACE IF NOT EXISTS test_ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}
        AND tablets = {'enabled': false};
        CREATE TABLE IF NOT EXISTS test_ks.kv (key int PRIMARY KEY, value text);
        INSERT INTO test_ks.kv (key, value) VALUES (1, 'docker-test');
    " 2>&1 || true

    if $CCM_CMD node2 cqlsh -e "SELECT * FROM test_ks.kv WHERE key=1;" 2>&1 | grep -q "docker-test"; then
        log_pass "Docker 3-node CQL: data replicated and readable from node2"
    else
        log_fail "Docker 3-node CQL: data not readable from node2"
    fi

    # Test node restart
    log_info "Test: node restart on Docker cluster"
    $CCM_CMD node3 stop 2>&1 || true
    sleep 3
    if $CCM_CMD node3 start 2>&1; then
        sleep 10
        if $CCM_CMD node3 nodetool status 2>&1 | grep -q "UN"; then
            log_pass "Docker node3 restart: back to UN"
        else
            log_fail "Docker node3 restart: not UN after restart"
        fi
    else
        log_fail "Docker node3 restart: failed to start"
    fi

    cleanup_cluster docker-test-3n
}

# ===================================================================
# Phase 3: Podman CLI tests
# ===================================================================
run_podman_tests() {
    log_section "Phase 3: Podman runtime tests"

    if ! command -v podman &>/dev/null; then
        log_skip "Podman not installed, skipping Podman tests"
        return
    fi

    # Pull image via podman
    log_info "Pulling image via Podman: docker.io/$SCYLLA_DOCKER_IMAGE"
    if podman pull "docker.io/$SCYLLA_DOCKER_IMAGE" 2>&1; then
        log_pass "Podman image pulled"
    else
        log_fail "Failed to pull image via Podman"
        return
    fi

    # 3a: 1-node Podman cluster with --container-runtime flag
    log_info "Test: 1-node Podman cluster (--container-runtime podman)"
    if $CCM_CMD create podman-test-1n --scylla -n 1 \
        --docker-image "$SCYLLA_DOCKER_IMAGE" \
        --container-runtime podman -s 2>&1; then
        log_pass "Podman 1-node cluster created and started"
    else
        log_fail "Podman 1-node cluster create failed"
        cleanup_cluster podman-test-1n
        return
    fi

    if $CCM_CMD status 2>&1 | grep -q "UP"; then
        log_pass "Podman 1-node cluster: node is UP"
    else
        log_fail "Podman 1-node cluster: node not UP"
        $CCM_CMD status 2>&1 || true
    fi

    # Test cqlsh on Podman
    if $CCM_CMD node1 cqlsh -e "DESCRIBE KEYSPACES;" 2>&1; then
        log_pass "Podman 1-node cqlsh works"
    else
        log_fail "Podman 1-node cqlsh failed"
    fi

    # Test nodetool on Podman
    if $CCM_CMD node1 nodetool status 2>&1 | grep -q "UN"; then
        log_pass "Podman 1-node nodetool status: UN"
    else
        log_fail "Podman 1-node nodetool status failed"
    fi

    cleanup_cluster podman-test-1n

    # 3b: 3-node Podman cluster
    log_info "Test: 3-node Podman cluster"
    if $CCM_CMD create podman-test-3n --scylla -n 3 \
        --docker-image "$SCYLLA_DOCKER_IMAGE" \
        --container-runtime podman -s 2>&1; then
        log_pass "Podman 3-node cluster created and started"
    else
        log_fail "Podman 3-node cluster create failed"
        cleanup_cluster podman-test-3n
        return
    fi

    local un_count
    un_count=$($CCM_CMD node1 nodetool status 2>&1 | grep -c "^UN" || echo "0")
    if [[ "$un_count" -eq 3 ]]; then
        log_pass "Podman 3-node nodetool: all 3 nodes UN"
    else
        log_fail "Podman 3-node nodetool: expected 3 UN nodes, got $un_count"
    fi

    cleanup_cluster podman-test-3n
}

# ===================================================================
# Phase 4: Environment variable runtime selection
# ===================================================================
run_env_var_tests() {
    log_section "Phase 4: CCM_CONTAINER_RUNTIME env var tests"

    # 4a: CCM_CONTAINER_RUNTIME=docker
    if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
        log_info "Test: CCM_CONTAINER_RUNTIME=docker"
        if CCM_CONTAINER_RUNTIME=docker $CCM_CMD create env-docker-test --scylla -n 1 \
            --docker-image "$SCYLLA_DOCKER_IMAGE" -s 2>&1; then
            log_pass "CCM_CONTAINER_RUNTIME=docker works"
        else
            log_fail "CCM_CONTAINER_RUNTIME=docker failed"
        fi
        cleanup_cluster env-docker-test
    else
        log_skip "Docker not available for env var test"
    fi

    # 4b: CCM_CONTAINER_RUNTIME=podman
    if command -v podman &>/dev/null; then
        log_info "Test: CCM_CONTAINER_RUNTIME=podman"
        if CCM_CONTAINER_RUNTIME=podman $CCM_CMD create env-podman-test --scylla -n 1 \
            --docker-image "$SCYLLA_DOCKER_IMAGE" -s 2>&1; then
            log_pass "CCM_CONTAINER_RUNTIME=podman works"
        else
            log_fail "CCM_CONTAINER_RUNTIME=podman failed"
        fi
        cleanup_cluster env-podman-test
    else
        log_skip "Podman not available for env var test"
    fi
}

# ===================================================================
# Phase 5: Pytest integration tests
# ===================================================================
run_pytest_integration() {
    log_section "Phase 5: Pytest integration tests"

    log_info "Running pytest Docker integration tests..."
    if SCYLLA_DOCKER_IMAGE="$SCYLLA_DOCKER_IMAGE" \
        uv run python -m pytest tests/test_docker_integration.py -x -m docker -v --tb=short 2>&1; then
        log_pass "Pytest Docker integration tests passed"
    else
        log_fail "Pytest Docker integration tests failed"
    fi
}

# ===================================================================
# Summary
# ===================================================================
print_summary() {
    log_section "Summary"
    echo -e "  ${GREEN}PASS${NC}: $PASS_COUNT"
    echo -e "  ${RED}FAIL${NC}: $FAIL_COUNT"
    echo -e "  ${YELLOW}SKIP${NC}: $SKIP_COUNT"
    echo ""

    if [[ "$FAIL_COUNT" -gt 0 ]]; then
        echo -e "${RED}Some tests failed!${NC}"
        return 1
    else
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    fi
}

# ===================================================================
# Main
# ===================================================================
main() {
    local mode="${1:-all}"

    preflight
    cleanup_all 2>/dev/null || true

    case "$mode" in
        unit)
            run_unit_tests
            ;;
        docker)
            run_docker_tests
            ;;
        podman)
            run_podman_tests
            ;;
        env)
            run_env_var_tests
            ;;
        pytest)
            run_pytest_integration
            ;;
        all)
            run_unit_tests
            run_docker_tests
            run_podman_tests
            run_env_var_tests
            run_pytest_integration
            ;;
        *)
            echo "Usage: $0 {all|unit|docker|podman|env|pytest}"
            exit 1
            ;;
    esac

    cleanup_all 2>/dev/null || true
    print_summary
}

main "$@"
