import prestodb
from src.params import PRESTO_PORT, PRESTO_CLUSTER_HOSTNAME, DB_KEYSPACE


class PrestoConnector(object):

    def __init__(self):
        self.conn = prestodb.dbapi.connect(
            host=PRESTO_CLUSTER_HOSTNAME,
            port=PRESTO_PORT,
            catalog='cassandra',
            schema=DB_KEYSPACE,
        )
        self.cur = self.conn.cursor()
