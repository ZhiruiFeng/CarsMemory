#!/usr/bin/env python3
# kafka.utils.py

"""Functions related to kafka communication"""
import src.kafka.settings as settings
from src.params import KAFKA_CLUSTER_HOSTNAME, KAFKA_BROKER, URL_PREFIX

import os
import numpy as np
import base64
import subprocess
from heapq import heappush, heappop


#######################
# Topic Manipulation
#######################

def get_topic_list():
    """Used to get topic list in Kafka cluster"""
    res = subprocess.check_output([os.path.join(settings.KAFKA_BIN,
                                                'kafka-topics.sh'),
                                   "--list",
                                   "--zookeeper",
                                   "{}:2181".format(KAFKA_CLUSTER_HOSTNAME)])
    res = res.decode("utf-8")
    topics = res.strip().split('\n')
    return topics


def topic_is_alive(topic):
    """Util function to check exists of topic
    :param topic: topic for checking
    """
    return topic in get_topic_list()


def clear_topic(topic):
    """Util function to clear frame topic.
    :param topic: topic to delete.
    """
    if not topic_is_alive(topic):
        return
    args = [os.path.join(settings.KAFKA_BIN, 'kafka-topics.sh'),
            "--zookeeper",
            "{}:2181".format(KAFKA_CLUSTER_HOSTNAME),
            "--delete",
            "--topic",
            str(topic)]
    os.system(" ".join(args))


def set_topic(topic, replication=2, partitions=settings.SET_PARTITIONS):
    """Util function to set topic.
    :param topic: topic to delete.
    :param partitions: set partitions.
    """
    # Check topic's existance
    if topic_is_alive(topic):
        clear_topic(topic)
    # SETTING UP TOPIC WITH DESIRED PARTITIONS
    args = [os.path.join(settings.KAFKA_BIN, 'kafka-topics.sh'),
            "--create",
            "--zookeeper",
            "{}:2181".format(KAFKA_CLUSTER_HOSTNAME),
            "--replication-factor",
            str(replication),
            "--partitions",
            str(partitions),
            "--topic",
            str(topic)]
    init_cmd = " ".join(args)
    print("\n", init_cmd, "\n")
    os.system(init_cmd)


def clear_detection_topics(prediction_prefix):
    """Clear detection topics. Specific to Camera Number.
    :param prediction_prefix: Just a stamp for this class of topics
    """
    for i in range(settings.TOTAL_CAMERAS + 1, 0, -1):
        # DELETE PREDICTION TOPICs, TO AVOID USING PREVIOUS JUNK DATA
        topic_name = prediction_prefix + '_' + str(i)
        clear_topic(topic_name)


#######################
# Format transform
#######################

def np_to_json(obj, prefix_name=""):
    """Serialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return {"{}_frame".format(prefix_name): base64.b64encode(obj.tostring()).decode("utf-8"),
            "{}_dtype".format(prefix_name): obj.dtype.str,
            "{}_shape".format(prefix_name): obj.shape}


def np_from_json(obj, prefix_name=""):
    """Deserialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return np.frombuffer(base64.b64decode(obj["{}_frame".format(prefix_name)].encode("utf-8")),
                         dtype=np.dtype(obj["{}_dtype".format(prefix_name)])).reshape(
        obj["{}_shape".format(prefix_name)])


#######################
# Consumers Template
#######################


#######################
# About S3 file access
#######################

def get_url_from_key(s3_key):
    """"Current we use this url to access file on S3 directly"""
    return URL_PREFIX + s3_key
