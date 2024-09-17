import pytest
import textwrap
from ccmlib.node import Node

# Define the test cases and corresponding outputs
test_cases = [
    {
        "id": "only_pending",
        "output": textwrap.dedent("""\
            pending tasks: 6
            - system_schema.tables: 1
            - system_schema.columns: 2
            - keyspace1.standard1: 3\
        """),
        "expected_tasks": [
            ("system_schema", "tables", 1),
            ("system_schema", "columns", 2),
            ("system_schema", None, 3),
            ("keyspace1", "standard1", 3),
            ("keyspace1", None, 3),
            (None, None, 6),
            ("keyspace1x", None, 0),
            ("keyspace1x", "table1x", 0),
        ]
    },
    {
        "id": "pending_and_in_progress",
        "output": textwrap.dedent("""\
            pending tasks: 6
            - system_schema.tables: 1
            - system_schema.columns: 2
            - keyspace1.standard1: 3

            id                                   compaction type keyspace      table   completed total unit progress
            8e1f2d90-a252-11ee-a7f4-1bf9ae4e6ffd COMPACTION      system_schema columns 1         640   keys 0.16%
            Active compaction remaining time :        n/a\
        """),
        "expected_tasks": [
            ("system_schema", "tables", 1),
            ("system_schema", "columns", 3),
            ("system_schema", None, 4),
            ("keyspace1", "standard1", 3),
            ("keyspace1", None, 3),
            (None, None, 7),
            ("keyspace1x", None, 0),
            ("keyspace1x", "table1x", 0),
        ]
    },
    {
        "id": "only_in_progress",
        "output": textwrap.dedent("""\
            pending tasks: 0

            id                                   compaction type keyspace      table   completed total unit progress
            8e1f2d90-a252-11ee-a7f4-1bf9ae4e6ffd COMPACTION      system_schema columns 1         640   keys 0.16%
            Active compaction remaining time :        n/a\
        """),
        "expected_tasks": [
            ("system_schema", "tables", 0),
            ("system_schema", "columns", 1),
            ("system_schema", None, 1),
            ("keyspace1", "standard1", 0),
            ("keyspace1", None, 0),
            (None, None, 1),
            ("keyspace1x", None, 0),
            ("keyspace1x", "table1x", 0),
        ]
    },
    {
        "id": "no_tasks",
        "output": textwrap.dedent("""\
            pending tasks: 0
            \
        """),
        "expected_tasks": [
            ("system_schema", "tables", 0),
            ("system_schema", "columns", 0),
            ("system_schema", None, 0),
            ("keyspace1", "standard1", 0),
            ("keyspace1", None, 0),
            (None, None, 0),
            ("keyspace1x", None, 0),
            ("keyspace1x", "table1x", 0),
        ]
    }
]

@pytest.mark.parametrize("test_case", test_cases, ids=[tc["id"] for tc in test_cases])
def test_parse_tasks(test_case):
    output = test_case["output"]
    expected_tasks = test_case["expected_tasks"]

    for ks, cf, expected in expected_tasks:
        n = Node._parse_tasks(output, ks, cf)
        assert n == expected, f"Expected {expected} tasks for {ks}.{cf}, but got {n}"
