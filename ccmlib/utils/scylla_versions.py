"""
Helpers for finding which ScyllaDB releases are current, based on the same
sources used by scylla-cluster-tests (SCT):

* the list of supported versions published in the ScyllaDB docs
* the version check service used by `scylla_setup`
"""
import argparse
import re
from functools import lru_cache

import requests
from packaging.version import Version

SUPPORTED_VERSIONS_URL = (
    "https://raw.githubusercontent.com/scylladb/scylladb-docs-homepage/main/docs/_static/data/supported_versions.json"
)
CHECK_VERSION_URL = "https://repositories.scylladb.com/scylla/check_version?system=scylla"


@lru_cache(maxsize=None)
def get_supported_scylla_versions(url: str = SUPPORTED_VERSIONS_URL) -> tuple[str, ...]:
    """
    Return the major versions marked as "Supported" in the ScyllaDB docs, newest first.

    e.g. ("2026.3", "2026.2", "2026.1", "2025.1")
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    supported = set()
    for entry in response.json().get("data", []):
        if entry.get("status", "").strip() != "Supported":
            continue
        # entries look like "ScyllaDB 2026.1" or "ScyllaDB 2025.1 (LTS)"
        if match := re.search(r"(\d{4}\.\d+)", entry.get("version", "")):
            supported.add(match.group(1))

    if not supported:
        raise ValueError(f"no supported ScyllaDB versions found in {url}")
    return tuple(sorted(supported, key=Version, reverse=True))


@lru_cache(maxsize=None)
def get_latest_scylla_release(url: str = CHECK_VERSION_URL) -> str:
    """
    Return the latest ScyllaDB release, as advertised by the service `scylla_setup` uses.

    e.g. "2026.3.1"
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()["version"]


def main():
    parser = argparse.ArgumentParser(description="Print current ScyllaDB release versions")
    parser.add_argument("what", choices=["latest", "supported"],
                        help="'latest' - the latest release (e.g. 2026.3.1), "
                             "'supported' - all supported major versions (e.g. 2026.3 2026.1)")
    args = parser.parse_args()

    if args.what == "latest":
        print(get_latest_scylla_release())
    else:
        print(" ".join(get_supported_scylla_versions()))


if __name__ == "__main__":
    main()
