from ccmlib.node import Node

def test_parse_pending_tasks():
    def verify_result(output, keyspace, column_family, expected):
        n = Node._parse_pending_tasks(output, keyspace, column_family)
        assert n == expected

    def verify_cases(output, cases):
        for ks, cf, expected in cases:
            verify_result(output, ks, cf, expected)

    for output in [
        '''
        pending tasks: 6
        - system_schema.tables: 1
        - system_schema.columns: 2
        - keyspace1.standard1: 3
        ''',
        '''
        pending tasks: 6
        - system_schema.tables: 1
        - system_schema.columns: 2
        - keyspace1.standard1: 3

        id                                   compaction type keyspace      table   completed total unit progress
        8e1f2d90-a252-11ee-a7f4-1bf9ae4e6ffd COMPACTION      system_schema columns 1         640   
    keys 0.16%
        Active compaction remaining time :        n/a
        '''
    ]:
        verify_cases(output, [
                        ('system_schema', 'tables', 1),
                        ('system_schema', 'columns', 2),
                        ('system_schema', None, 3),
                        ('keyspace1', 'standard1', 3),
                        ('keyspace1', None, 3),
                        (None, None, 6),
                        ('keyspace1x', None, 0),
                        ('keyspace1x', 'table1x', 0),
                     ])
