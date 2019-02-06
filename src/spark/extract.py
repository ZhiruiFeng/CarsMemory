from __future__ import print_function

import sys
import json
from pyspark import SparkContext
from pyspark.stream import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from src.params import KAFKA_BROKER
from src.processing.keyframes import parse_mapper


def calc_diff(a, b):
    print(a)
    if a[1]['timestamp'] < b[1]['timestamp']:
        domain = a[1].copy()
    else:
        domain = b[1].copy()
    domain['is_keyframe'] = (a[1]['objs'] != b[1]['objs'])
    return domain


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage extract.py <dashcam_id>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="KafkaExtractor")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 60)  # Batch duration

    cam_id = str(sys.argv[1])

    brokers = KAFKA_BROKER
    group_name = "spark-extractor"
    topic = "extract_" + cam_id

    # Since extract need to aggregte frames of same camera from partitions
    # the partitions of topic should be one
    topic_partitions = 1

    kafkaStream = KafkaUtils.createStream(ssc,
                                          brokers,
                                          group_name,
                                          {topic: topic_partitions})

    parsed = kafkaStream \
        .map(lambda v: json.loads(v[1])) \
        .map(lambda msginfo: parse_mapper(msginfo))

    # This information is for adjusting batch duration
    parsed.count() \
          .map(lambda x: '['+cam_id+'] Frames in this batch: {}'.format(x)) \
          .pprint()

    # Transfer to batch and sort it with timestamp
    #
    result = parsed.transform(lambda foo: foo.sortBy(lambda x: (x['timestamp']))) \
                   .zipWithIndex() \
                   .flatMap(lambda (key, index): [((str(index) + '_' + str(index+1)), key),
                                                  ((str(index+1) + '_' + str(index+2)), key)]) \
                   .reduce(lambda a, b: calc_diff(a, b)) \
                   .pprint()

    ssc.start()
    ssc.awaitTermination()
