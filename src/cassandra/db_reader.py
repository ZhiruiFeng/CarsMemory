#!/usr/bin/env python3
# db_reader.py

"""Read meta data from Cassandra"""

from db_connector import CassandraConnector


class DBReader(object):

    def __init__(self):
        self.db = CassandraConnector()
        self.session = self.db.get_session()
