#!/usr/bin/env python

from cassandra.cluster import Cluster,Session
import weakref
from weakref import WeakValueDictionary
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA
import argparse


class SimpleCluster(Cluster):
    def simple_connect(self, ip):
        self.connection_class.initialize_reactor()
        connection = self.connection_factory(ip, is_control_connection=False)
        self.add_host(ip, 'dc1', 'rack1', False, False)
        self.load_balancing_policy.populate(
                 weakref.proxy(self), self.metadata.all_hosts())
        session = Session(self, self.metadata.all_hosts())
        self.sessions.add(session)
        return session

parser = argparse.ArgumentParser(description="Enable single node drivers")
parser.add_argument('ip', nargs='?',default='127.0.0.1', help="ip address of node")
args = parser.parse_args()

scluster = SimpleCluster();
session = scluster.simple_connect(args.ip);
session.set_keyspace('system');
session.execute("insert into system.local (key,cluster_name,data_center,rack,partitioner,schema_version,tokens) values (\'local\',\'test\',\'datacenter1\',\'rack1\',\'org.apache.cassandra.dht.Murmur3Partitioner\',uuid(),{\'-1020451731157648594\', \'-121555383949837090\'});");

cluster = Cluster([args.ip])
session2 = cluster.connect();
#rows = session2.execute("select key, rack from system.local");
#for e in rows:
#    print e.key, e.rack; 
