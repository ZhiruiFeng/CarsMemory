"""This type of consumer is for extracting keyframes"""

import socket
import json
import src.params as params
import src.kafka.settings as settings
from src.processing.objdetector import detect_object
from src.kafka.utils import get_url_from_key

from multiprocessing import Process
from kafka import KafkaConsumer, KafkaProducer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition
from src.processing.keyframes import parse_objs, parse_scene
from collections import Counter
import time

class Extractor(Process):

    def __init__(self,
                 meta_topic,
                 value_topic,
                 topic_partitions=8,
                 verbose=False,
                 rr_distribute=False,
                 group_id="extractor",
                 group=None,
                 target=None,
                 name=None):
        """
        OBJECT DETECTION IN FRAMES --> Consuming encoded frame messages, detect faces and their encodings [PRE PROCESS],
        publish it to processed_frame_topic where these values are used for face matching with query faces.
        :param meta_topic:
        :param value_topic:
        :param topic_partitions: number of partitions processed_frame_topic topic has, for distributing messages among partitions
        :param verbose: print logs on stdout
        :param rr_distribute:  use round robin partitioner and assignor, should be set same as respective producers or consumers.
        :param group_id: kafka used to attribute topic partition
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        """

        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.meta_topic = meta_topic

        self.verbose = verbose
        self.topic_partitions = topic_partitions
        self.value_topic = value_topic
        self.rr_distribute = rr_distribute
        self.group_id = group_id
        self.counter = Counter()
        self.timer = time.time()
        print("[INFO] I am ", self.iam)

    def run(self):
        """Consume raw frames, detects faces, finds their encoding [PRE PROCESS],
           predictions Published to processed_frame_topic fro face matching."""

        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        meta_consumer = KafkaConsumer(group_id=self.group_id, client_id=self.iam,
                                      bootstrap_servers=[params.KAFKA_BROKER],
                                      key_deserializer=lambda key: key.decode(),
                                      value_deserializer=lambda value: json.loads(value.decode()),
                                      partition_assignment_strategy=partition_assignment_strategy,
                                      auto_offset_reset="earliest")

        meta_consumer.subscribe([self.meta_topic])

        # partitioner for processed frame topic
        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.value_topic, partition=i)
                                                 for i in range(self.topic_partitions)])

        else:
            partitioner = Murmur2Partitioner(partitions=
                                             [TopicPartition(topic=self.value_topic, partition=i)
                                              for i in range(self.topic_partitions)])

        #  Produces prediction object
        value_producer = KafkaProducer(bootstrap_servers=[params.KAFKA_BROKER],
                                       key_serializer=lambda key: str(key).encode(),
                                       value_serializer=lambda value: json.dumps(value).encode(),
                                       partitioner=partitioner)
        history_cnt = None
        try:
            while True:

                meta_messages = meta_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partition, msgs in meta_messages.items():

                    if time.time() - self.timer > 600:
                        self.update_acc_table()
                        self.counter = Counter()
                        self.timer = time.time()

                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        # get pre processing result
                        # TODO need to add a buffer for ordering message from different partitions
                        result = msg.value
                        new_obj_format, new_cnt = parse_objs(result['objs'])
                        scene_lists = parse_scene(result['scenes'], 3)
                        result['objs'] = new_obj_format
                        result['scenes'] = scene_lists
                        # A easy version of key_frame extractor
                        is_keyframe = False
                        if not history_cnt:
                            is_keyframe = True
                        elif not history_cnt == new_cnt:
                            is_keyframe = True

                        self.counter += new_cnt

                        result['is_keyframe'] = is_keyframe
                        result['valuable'] = True
                        if self.verbose:
                            print("[Extractor done]")
                            print(result)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        meta_consumer.commit(offsets=offsets)

                        # Partition to be sent to
                        value_producer.send(self.value_topic, value=result)

                    value_producer.flush()

        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            meta_consumer.close()

    def update_acc_table(self):
        pass
