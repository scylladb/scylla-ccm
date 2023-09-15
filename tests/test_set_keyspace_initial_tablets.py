import pytest
from typing import List, Optional

from ccmlib.node import Node


@pytest.mark.parametrize("stress_options, initial_tablets, datacenter, rf, expected", [
    # typical case
    (['write', 'n=0', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1'], 8, None, 3,
     ['write', 'n=0', 'no-warmup', '-schema', 'replication(strategy=NetworkTopologyStrategy,datacenter1=1,initial_tablets=8)', '-rate', 'threads=1']),
    # no replication
    (['write', 'n=3500K', '-rate', 'threads=50', '-schema', 'compaction(strategy=SizeTieredCompactionStrategy)'], 8, None, 3,
     ['write', 'n=3500K', '-rate', 'threads=50', '-schema', 'replication(strategy=NetworkTopologyStrategy,datacenter1=3,initial_tablets=8)', 'compaction(strategy=SizeTieredCompactionStrategy)']),
    # replication and keyspace provided in one param
    (['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=4) keyspace=ks1'], 8, None, 3,
     ['write', 'n=20000', 'no-warmup', '-schema', 'replication(strategy=NetworkTopologyStrategy,datacenter1=4,initial_tablets=8) keyspace=ks1']),
    # replication in one str with -schema
    (['write', 'cl=QUORUM', 'n=100', "-schema replication(factor=3)", "-mode cql3 native", "-rate threads=10", "-pop seq=1..100"], 8, None, 3,
     ['write', 'cl=QUORUM', 'n=100', "-schema", 'replication(strategy=NetworkTopologyStrategy,datacenter1=3,initial_tablets=8)', "-mode cql3 native", "-rate threads=10", "-pop seq=1..100"]),
    # Different number of initial_tablets
    (['write', 'n=0', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1'], 10, None, 3,
     ['write', 'n=0', 'no-warmup', '-schema', 'replication(strategy=NetworkTopologyStrategy,datacenter1=1,initial_tablets=10)', '-rate',
      'threads=1']),
    # Different datacenter
    (['write', 'n=0', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1'], 8, 'datacenter2', 3,
     ['write', 'n=0', 'no-warmup', '-schema', 'replication(strategy=NetworkTopologyStrategy,datacenter2=1,initial_tablets=8)', '-rate',
      'threads=1'])
])
def test_set_keyspace_initial_tablets(stress_options: List[str], initial_tablets:int, datacenter: Optional[str], rf: int, expected: List[str]):
    assert Node._set_keyspace_initial_tablets(stress_options, initial_tablets, datacenter, rf) == expected
