from unittest.mock import MagicMock, patch

import pytest

from ccmlib.utils.scylla_versions import get_supported_scylla_versions, get_latest_scylla_release

SUPPORTED_VERSIONS_JSON = {
    "data": [
        {"version": "ScyllaDB 2026.2", "status": "Supported"},
        {"version": "ScyllaDB 2026.3", "status": "Supported"},
        {"version": "ScyllaDB 2026.1 (LTS)", "status": "Supported"},
        {"version": "ScyllaDB 2025.4", "status": "Not supported"},
        {"version": "ScyllaDB 2025.3", "status": " Not supported"},
        {"version": "ScyllaDB 2025.1 (LTS)", "status": "Supported "},
        {"version": "Enterprise 2024.2", "status": "Not supported"},
        {"version": "Open Source 6.2", "status": "Not supported"},
    ]
}


def mock_response(json_data):
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = json_data
    return patch("ccmlib.utils.scylla_versions.requests.get", return_value=response)


@pytest.fixture(autouse=True)
def clear_caches():
    get_supported_scylla_versions.cache_clear()
    get_latest_scylla_release.cache_clear()
    yield
    get_supported_scylla_versions.cache_clear()
    get_latest_scylla_release.cache_clear()


def test_get_supported_scylla_versions():
    with mock_response(SUPPORTED_VERSIONS_JSON):
        assert get_supported_scylla_versions() == ("2026.3", "2026.2", "2026.1", "2025.1")


def test_get_supported_scylla_versions_none_supported():
    with mock_response({"data": [{"version": "Enterprise 2024.2", "status": "Not supported"}]}):
        with pytest.raises(ValueError, match="no supported ScyllaDB versions"):
            get_supported_scylla_versions()


def test_get_latest_scylla_release():
    with mock_response({"version": "2026.3.1", "latest_patch_version": "2026.3.1"}):
        assert get_latest_scylla_release() == "2026.3.1"
