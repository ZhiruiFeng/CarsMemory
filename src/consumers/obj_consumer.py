"""This type of consumer is for object detection"""

import socket
import json
import src.params as params
import src.kafka.settings as settings
from src.processing.objdetector import detect_object
from src.processing.scenedetector import detect_scene_algorithmia
from src.kafka.utils import get_url_from_key

from multiprocessing import Process
from kafka import KafkaConsumer, KafkaProducer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition
import time


class ObjConsumer(Process):

    def __init__(self,
                 url_topic,
                 obj_topic,
                 topic_partitions=8,
                 verbose=False,
                 rr_distribute=False,
                 group_id="objdetector",
                 group=None,
                 target=None,
                 name=None):
        """
        OBJECT DETECTION IN FRAMES --> Consuming encoded frame messages, detect faces and their encodings [PRE PROCESS],
        publish it to processed_frame_topic where these values are used for face matching with query faces.
        :param url_topic:
        :param obj_topic: It's just a prefix which need to accompany with the camera id.
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
        self.url_topic = url_topic

        self.verbose = verbose
        self.topic_partitions = topic_partitions
        self.obj_topic = obj_topic
        self.rr_distribute = rr_distribute
        self.group_id = group_id
        self.timer = time.time()
        self.cnt = -1
        self.latency_period = 10
        print("[INFO] I am ", self.iam)

    def run(self):
        """Consume raw frames, detects faces, finds their encoding [PRE PROCESS],
           predictions Published to processed_frame_topic fro face matching."""

        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        url_consumer = KafkaConsumer(group_id=self.group_id, client_id=self.iam,
                                     bootstrap_servers=[params.KAFKA_BROKER],
                                     key_deserializer=lambda key: key.decode(),
                                     value_deserializer=lambda value: json.loads(value.decode()),
                                     partition_assignment_strategy=partition_assignment_strategy,
                                     auto_offset_reset="earliest")

        url_consumer.subscribe([self.url_topic])

        # partitioner for processed frame topic
        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.obj_topic, partition=i)
                                                 for i in range(self.topic_partitions)])

        else:
            partitioner = Murmur2Partitioner(partitions=
                                             [TopicPartition(topic=self.obj_topic, partition=i)
                                              for i in range(self.topic_partitions)])

        #  Produces prediction object
        obj_producer = KafkaProducer(bootstrap_servers=[params.KAFKA_BROKER],
                                     key_serializer=lambda key: str(key).encode(),
                                     value_serializer=lambda value: json.dumps(value).encode(),
                                     partitioner=partitioner)
        try:
            while True:

                if self.verbose:
                    print("[ConsumeFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))

                raw_frame_messages = url_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partition, msgs in raw_frame_messages.items():

                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        # get pre processing result
                        if self.cnt < 0:
                            self.timer = time.time()
                            self.cnt = 0
                        result = self.get_processed_frame_object(msg.value)

                        # Calculate latency:
                        self.cnt += 1

                        if self.cnt == self.latency_period:
                            latency = (time.time() - self.timer) / float(self.latency_period)
                            self.timer = time.time()
                            self.cnt = 0
                            print("[Detection] Latency {}".format(latency))

                        if self.verbose:
                            print(result)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        url_consumer.commit(offsets=offsets)

                        # Partition to be sent to
                        send_topic = self.obj_topic + '_' + str(result['camera'])
                        obj_producer.send(send_topic, value=result)

                    obj_producer.flush()

        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            url_consumer.close()

    @staticmethod
    def get_processed_frame_object(msginfo):
        """Processes value produced by producer, returns prediction with png image.
        :param s3_key: how to access the image
        :return: A dict updated with objects found in that frame, i.e. their location and encoding.
        """
        s3_key = msginfo['s3_key']
        s3_url = get_url_from_key(s3_key)
        # To detect objects
        objs = detect_object(s3_url)
        # To detect secenes
        scene = detect_scene_algorithmia(s3_url)
        msginfo['objs'] = objs
        msginfo['scenes'] = scene['predictions']
        return msginfo
