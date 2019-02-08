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
from src.processing.keyframes import parse_objs, parse_scene, parse_mapper
from collections import Counter
import time
from heapq import heappush, heappop


class Extractor(Process):

    def __init__(self,
                 meta_topic,
                 value_topic,
                 topic_partitions=1,
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
        self.buffer = []
        self.uppersize = 50
        self.scenemapper = {}
        self.keyframe_cnt = 0
        self.load_scene_mapper()

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
                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        # get pre processing result
                        result = parse_mapper(msg.value)
                        self.transfer_scene_type(result)

                        # Using the buffer to keep time order
                        heappush(self.buffer, (result['frame_num'], result))

                        if len(self.buffer) < self.uppersize:
                            continue

                        result = heappop(self.buffer)[1]

                        # Extract keyframe
                        if history_cnt is None:
                            result['is_keyframe'] = True
                        else:
                            new_cnt = Counter(result['counts'])
                            result['is_keyframe'] = (new_cnt != history_cnt)
                            history_cnt = new_cnt

                        # Scene statistic
                        scenecnt = Counter(result['scenes'])
                        self.counter += scenecnt
                        if result['is_keyframe']:
                            self.keyframe_cnt += 1

                        # Need to be refined later
                        result['valuable'] = self.is_valuable(result)

                        # Update some statistic informations every minute
                        if time.time() - self.timer > 60:
                            self.update_acc_table(result)

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

    def update_acc_table(self, msginfo):
        # Just insert command for dbsinker to do the job.
        msginfo['update_statistic'] = True
        msginfo['update_scene_cnt'] = dict(self.counter)
        msginfo['update_keyframe_cnt'] = dict(self.keyframe_cnt)
        self.counter = Counter()
        self.keyframe_cnt = 0
        self.timer = time.time()

    def is_valuable(self, msginfo):
        return True

    def load_scene_mapper(self):
        with open('../scenelist.txt', 'r') as f:
            content = f.readlines()
            for line in content:
                items = line.strip().split(' ')
                type = items[0]
                for scene in items[1:]:
                    self.scenemapper[scene] = type

    def transfer_scene_type(self, msginfo):
        scenes = msginfo['scenes']
        displayset = set()
        for scene in scenes:
            if scene in self.scenemapper:
                displayset.add(self.scenemapper[scene])
        msginfo['scenes'] = list[displayset]
