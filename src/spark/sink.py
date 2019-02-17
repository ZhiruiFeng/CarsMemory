from __future__ import print_function

import json
from pyspark import SparkContext
from pyspark.stream import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from src.params import KAFKA_BROKER
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from src.cassandra.db_writer import insert_frame_command, update_statistic_command
from src.params import DB_CLUSTER_HOSTNAME, CASSANDRA_PORT, DB_KEYSPACE


def sink_to_db(msg):
    """According to the massage info to write to database"""
    cluster = Cluster([DB_CLUSTER_HOSTNAME], port=CASSANDRA_PORT)
    session = cluster.connect(DB_KEYSPACE)
    insert_frame = insert_frame_command(msg)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch.add(insert_frame)
    if 'update_statistic' in msg:
        update_statistic = update_statistic_command(msg)
        batch.add(update_statistic)

    session.execute(batch)
    cluster.shutdown()


if __name__ == "__main__":

    sc = SparkContext(appName="KafkaDBSinker")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)  # Batch duration

    brokers = KAFKA_BROKER
    topic = "sink"

    kafkaStream = KafkaUtils.createDirectStream(ssc,
                                                topic,
                                                {"metadata.broker.list": KAFKA_BROKER})
    # Get valuable data
    parsed = kafkaStream \
        .map(lambda v: json.loads(v[1])) \
        .filter(lambda v: v['valuable'])

    # Save to datebase
    save = parsed.map(sink_to_db)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
