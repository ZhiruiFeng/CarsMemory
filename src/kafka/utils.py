#!/usr/bin/env python3
# kafka.utils.py

"""Functions related to kafka communication"""
import settings
import os
import numpy as np
import base64
import json


def clear_topic(topic):
    """Util function to clear frame topic.
    :param topic: topic to delete.
    """
    os.system(settings.KAFKA_HOME_BIN + "kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}".format(topic))


def set_topic(topic, replication=2, partitions=settings.SET_PARTITIONS):
    """Util function to set topic.
    :param topic: topic to delete.
    :param partitions: set partitions.
    """
    # SETTING UP TOPIC WITH DESIRED PARTITIONS
    init_cmd = settings.KAFKA_HOME_BIN + "kafka-topics.sh --create --zookeeper localhost:2181 " \
               "--replication-factor {} --partitions {} --topic {}".format(replication, partitions, topic)

    print("\n", init_cmd, "\n")
    os.system(init_cmd)


def clear_detection_topics(prediction_prefix=settings.PREDICTION_TOPIC_PREFIX):
    """Clear detection topics. Specific to Camera Number.
    :param prediction_prefix: Just a stamp for this class of topics
    """

    for i in range(settings.TOTAL_CAMERAS + 1, 0, -1):
        print()
        # DELETE PREDICTION TOPICs, TO AVOID USING PREVIOUS JUNK DATA
        os.system(settings.KAFKA_HOME_BIN + "kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}_{}".format(
            prediction_prefix, i))


# G. 1.
def np_to_json(obj, prefix_name=""):
    """Serialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return {"{}_frame".format(prefix_name): base64.b64encode(obj.tostring()).decode("utf-8"),
            "{}_dtype".format(prefix_name): obj.dtype.str,
            "{}_shape".format(prefix_name): obj.shape}


# G. 2.
def np_from_json(obj, prefix_name=""):
    """Deserialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return np.frombuffer(base64.b64decode(obj["{}_frame".format(prefix_name)].encode("utf-8")),
                         dtype=np.dtype(obj["{}_dtype".format(prefix_name)])).reshape(
        obj["{}_shape".format(prefix_name)])
