#!/usr/bin/env python3
# db_connector.py

"""Connector to Cassandra"""

from cassandra.cluster import Cluster
from ..params import DB_CLUSTER_HOSTNAME, KEYSPACE, CASSANDRA_PORT


class CassandraConnector(object):

    def __init__(self):
        self.cluster = Cluster([DB_CLUSTER_HOSTNAME], port=CASSANDRA_PORT)
        self.session = self.cluster.connect(KEYSPACE)

    def execute(self, sql_command):
        return self.session.execute(sql_command)
