from __future__ import print_function

import json
from pyspark import SparkContext
from pyspark.stream import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from src.params import KAFKA_BROKER
from src.cassandra.db_writer import insert_frame_command


if __name__ == "__main__":

    sc = SparkContext(appName="KafkaDBSinker")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)  # Batch duration

    brokers = KAFKA_BROKER
    group_name = "spark-sinker"
    topic = "sink"
    topic_partitions = 16

    kafkaStream = KafkaUtils.createStream(ssc,
                                          brokers,
                                          group_name,
                                          {topic: topic_partitions})
    # Partitions
    parsed = kafkaStream \
        .map(lambda v: json.loads(v[1])) \
        .filter(lambda v: v['valuable'])

    parsed.pprint()

    scene_count = parsed \
        .flatMap(lambda msginfo: msginfo['scenes']) \
        .countByValue()

    # Some objects feature calculation:
    # car_count
    # person_count
    # traffic_sign_count

    # Insert to frames
    frames_insert_commands = parsed \
        .map(lambda msginfo: insert_frame_command(msginfo)) \


    # Next save to cassandra.

    ssc.start()
    ssc.awaitTermination()
