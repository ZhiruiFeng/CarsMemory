from __future__ import print_function

import json
from pyspark import SparkContext
from pyspark.stream import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from src.params import KAFKA_BROKER
from src.cassandra.db_writer import insert_frame_command


def calc_diff(a, b):
    print(a)
    if a[1]['timestamp'] < b[1]['timestamp']:
        domain = a[1].copy()
    else:
        domain = b[1].copy()
    domain['is_keyframe'] = (a[1]['objs'] == b[1]['objs'])
    return domain


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
        .zipWithIndex() \
        .flatMap(lambda (key, index): [((str(index) + '_' + str(index+1)), key),
                                       ((str(index+1) + '_' + str(index+2)), key)]) \
        .reduce(lambda a, b: calc_diff(a, b)) \
        .pprint()

    # Next save to cassandra.

    ssc.start()
    ssc.awaitTermination()
