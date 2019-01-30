#!/usr/bin/env python3
# db_connector.py

"""Connector to Cassandra"""

from cassandra.cluster import Cluster
from ..params import DB_CLUSTER_HOSTNAME, CASSANDRA_PORT, DB_KEYSPACE


class CassandraConnector(object):

    def __init__(self):
        self.cluster = Cluster([DB_CLUSTER_HOSTNAME], port=CASSANDRA_PORT)
        self.session = self.cluster.connect(DB_KEYSPACE)

    def get_session(self):
        return self.session
