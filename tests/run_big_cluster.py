"""Start a large podman cluster with configurable topology and concurrency.

Usage:
  python tests/run_big_cluster.py [--concurrency N] [--nodes DC:AZ:N ...]
"""

import concurrent.futures
import logging
import os
import sys
import tempfile
import time
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("run_big_cluster")

for name in ("ccmlib.scylla_podman_cluster", "ccmlib.node", "ccmlib.cluster"):
    logging.getLogger(name).setLevel(logging.WARNING)


def main():
    import shutil

    if not shutil.which("podman"):
        print("podman not found", file=sys.stderr)
        return 1

    image = os.environ.get(
        "SCYLLA_PODMAN_IMAGE",
        os.environ.get("SCYLLA_DOCKER_IMAGE", "docker.io/scylladb/scylla-nightly:latest"),
    )

    topology = {"dc1": {"rack1": 5, "rack2": 5, "rack3": 5},
                "dc2": {"rack1": 5, "rack2": 5, "rack3": 5},
                "dc3": {"rack1": 5, "rack2": 5, "rack3": 5}}

    node_count = sum(sum(racks.values()) for racks in topology.values())
    concurrency = 2

    print(f"Image: {image}")
    print(f"Topology: {node_count} nodes across {len(topology)} DCs")
    print(f"Concurrency: {concurrency}")
    print()

    from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

    test_dir = tempfile.mkdtemp(prefix="ccm-bigcluster-")
    cluster = ScyllaPodmanCluster(
        str(test_dir),
        name="bigcluster",
        podman_image=image,
        inter_dc_delay_ms=40,
        inter_rack_delay_ms=1,
    )

    cluster.populate(topology)

    total_nodes = len(cluster.nodelist())
    print(f"Populated {total_nodes} nodes")

    cluster.set_configuration_options(values={
        "read_request_timeout_in_ms": 10000,
        "range_request_timeout_in_ms": 10000,
        "write_request_timeout_in_ms": 10000,
        "truncate_request_timeout_in_ms": 10000,
        "request_timeout_in_ms": 10000,
    })

    nodes = cluster.nodelist()
    t0 = time.time()

    def start_node(node):
        LOG.info("Starting %s ...", node.name)
        node.start(wait_for_binary_proto=True, wait_other_notice=False)
        LOG.info("Started %s", node.name)
        return node

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(start_node, n): n for n in nodes}
        done = 0
        for future in concurrent.futures.as_completed(futures):
            n = futures[future]
            done += 1
            try:
                future.result()
                elapsed = time.time() - t0
                LOG.info(
                    "[%d/%d] %s UP in %.1f s",
                    done, total_nodes, n.name, elapsed,
                )
            except Exception as exc:
                LOG.error("%s FAILED: %s", n.name, exc)
                traceback.print_exc()

    total = time.time() - t0
    print(f"\nTotal time: {total:.1f} s ({total/60:.1f} min)")

    running = sum(1 for n in nodes if n.is_running())
    print(f"Nodes UP: {running}/{total_nodes}")

    print("\nCleaning up...")
    cluster.remove()
    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
