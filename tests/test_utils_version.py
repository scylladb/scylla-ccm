import pytest

from ccmlib.utils.version import ComparableScyllaVersion

@pytest.mark.parametrize("version_string, expected", (
    ("5.1", (5, 1, 0, '', '')),
    ("5.1.0", (5, 1, 0, '', '')),
    ("5.1.1", (5, 1, 1, '', '')),
    ("5.1.0-rc1", (5, 1, 0, 'rc1', '')),
    ("5.1.0~rc1", (5, 1, 0, 'rc1', '')),
    ("5.1.rc1", (5, 1, 0, 'rc1', '')),
    ("2022.1.3-0.20220922.539a55e35", (2022, 1, 3, "dev-0.20220922", "539a55e35")),
    ("2022.1.3-0.20220922.539a55e35 with build-id d1fb2faafd95058a04aad30b675ff7d2b930278d",
     (2022, 1, 3, "dev-0.20220922", "539a55e35")),
    ("2022.1.3-dev-0.20220922.539a55e35", (2022, 1, 3, "dev-0.20220922", "539a55e35")),
    ("5.2.0~rc1-0.20230207.8ff4717fd010", (5, 2, 0, "rc1-0.20230207", "8ff4717fd010")),
    ("5.2.0-dev-0.20230109.08b3a9c786d9", (5,  2, 0, "dev-0.20230109", "08b3a9c786d9")),
    ("5.2.0-dev-0.20230109.08b3a9c786d9-x86_64", (5, 2, 0, "dev-0.20230109", "08b3a9c786d9")),
    ("5.2.0-dev-0.20230109.08b3a9c786d9-aarch64", (5, 2, 0, "dev-0.20230109", "08b3a9c786d9")),
    ("2024.2.0.dev.0.20231219.c7cdb16538f2.1", (2024, 2, 0, "dev-0.20231219", "c7cdb16538f2.1")),
    ("2024.1.0.rc2.0.20231218.a063c2c16185.1", (2024, 1, 0, "rc2-0.20231218", "a063c2c16185.1")),
    ("2.6-dev-0.20211108.5f1e01cbb34-SNAPSHOT-5f1e01cbb34", (2, 6, 0, "dev.0.20211108", '5f1e01cbb34.SNAPSHOT.5f1e01cbb34')),
))
def test_comparable_scylla_version_init_positive(version_string, expected):
    comparable_scylla_version = ComparableScyllaVersion(version_string)
    assert comparable_scylla_version.v_major == expected[0]
    assert comparable_scylla_version.v_minor == expected[1]
    assert comparable_scylla_version.v_patch == expected[2]
    assert comparable_scylla_version.v_pre_release == expected[3]
    assert comparable_scylla_version.v_build == expected[4]


@pytest.mark.parametrize("version_string", (None, "", "5", "2023", "2023.dev"))
def test_comparable_scylla_versions_init_negative(version_string):
    try:
        ComparableScyllaVersion(version_string)
    except ValueError:
        pass
    else:
        assert False, (
            f"'ComparableScyllaVersion' must raise a ValueError for the '{version_string}' "
            "provided input")


def _compare_versions(version_string_left, version_string_right,
                      is_left_greater, is_equal, comparable_class):
    comparable_version_left = comparable_class(version_string_left)
    comparable_version_right = comparable_class(version_string_right)

    compare_expected_result_err_msg = (
        "One of 'is_left_greater' and 'is_equal' must be 'True' and another one must be 'False'")
    assert is_left_greater or is_equal, compare_expected_result_err_msg
    assert not (is_left_greater and is_equal)
    if is_left_greater:
        assert comparable_version_left > comparable_version_right
        assert comparable_version_left >= comparable_version_right
        assert comparable_version_left > version_string_right
        assert comparable_version_left >= version_string_right
        assert comparable_version_right < comparable_version_left
        assert comparable_version_right <= comparable_version_left
        assert comparable_version_right < version_string_left
        assert comparable_version_right <= version_string_left
    else:
        assert comparable_version_left == comparable_version_right
        assert comparable_version_left == version_string_right
        assert comparable_version_left >= comparable_version_right
        assert comparable_version_left >= version_string_right
        assert comparable_version_right <= comparable_version_left
        assert comparable_version_right <= version_string_left


@pytest.mark.parametrize(
    "version_string_left, version_string_right, is_left_greater, is_equal, comparable_class", (
        ("5.2.2", "5.2.2", False, True, ComparableScyllaVersion),
        ("5.2.0", "5.1.2", True, False, ComparableScyllaVersion),
        ("5.2.1", "5.2.0", True, False, ComparableScyllaVersion),
        ("5.2.10", "5.2.9", True, False, ComparableScyllaVersion),
        ("5.2.0", "5.2.0~rc1-0.20230207.8ff4717fd010", True, False, ComparableScyllaVersion),
        ("5.2.0", "5.2.0-dev-0.20230109.08b3a9c786d9", True, False, ComparableScyllaVersion),
        ("2023.1.0", "2023.1.rc1", True, False, ComparableScyllaVersion),
        ("5.2.0", "5.1.rc1", True, False, ComparableScyllaVersion),
        ("5.2.0-dev-0.20230109.8ff4717fd010", "5.2.0-dev-0.20230109.08b3a9c786d9",
         False, True, ComparableScyllaVersion),
    ))
def test_comparable_scylla_versions_compare(version_string_left, version_string_right,
                                            is_left_greater, is_equal, comparable_class):
    _compare_versions(
        version_string_left, version_string_right, is_left_greater, is_equal, comparable_class)


@pytest.mark.parametrize("version_string_input, version_string_output", (
    ("5.2.2", "5.2.2"),
    ("2023.1.13", "2023.1.13"),
    ("5.2.0~rc0-0.20230207", "5.2.0-rc0-0.20230207"),
    ("5.2.0-rc1-0.20230207", "5.2.0-rc1-0.20230207"),
    ("5.2.0~dev-0.20230207.8ff4717fd010", "5.2.0-dev-0.20230207+8ff4717fd010"),
))
def test_comparable_scylla_versions_to_str(version_string_input, version_string_output):
    assert str(ComparableScyllaVersion(version_string_input)) == version_string_output