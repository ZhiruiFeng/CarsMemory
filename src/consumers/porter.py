"""This type of consumer is to carry raw_data to S3 temporary"""

import json
import cv2
import numpy as np
import socket
from multiprocessing import Process

from src.kafka.utils import np_from_json
import src.params as params
import src.kafka.settings as settings
from src.awss3.writer_s3 import S3TmpWriter

from kafka import KafkaConsumer, KafkaProducer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition


class Porter(Process):

    def __init__(self,
                 frame_topic,
                 url_topic,
                 topic_partitions=8,
                 verbose=False,
                 rr_distribute=False,
                 group_id="porter",
                 group=None,
                 target=None,
                 name=None):
        """
        To store frames from frame_topic on S3, and publish the key with
        feature back to url_topics.
        :param frame_topic: kafka topic to consume stamped encoded frames.
        :param url_topic: kafka topic to publish object keys on S3
        :param topic_partitions: number of partitions processed_frame_topic topic has, for distributing messages among partitions
        :param scale: (0, 1] scale image before face recognition, but less accurate, trade off!!
        :param verbose: print logs on stdout
        :param rr_distribute:  use round robin partitioner and assignor, should be set same as respective producers or consumers.
        :param group_id: kafka used to attribute topic partition
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        """
        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = frame_topic

        self.verbose = verbose
        self.topic_partitions = topic_partitions
        self.url_topic = url_topic
        self.rr_distribute = rr_distribute
        self.group_id = group_id
        print("[INFO] I am ", self.iam)
        self.s3writer = S3TmpWriter(params.MY_BUCKET)

    def run(self):
        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        porter_consumer = KafkaConsumer(group_id=self.group_id, client_id=self.iam,
                                        bootstrap_servers=[params.KAFKA_BROKER],
                                        key_deserializer=lambda key: key.decode(),
                                        value_deserializer=lambda value: json.loads(value.decode()),
                                        partition_assignment_strategy=partition_assignment_strategy,
                                        auto_offset_reset="earliest")

        porter_consumer.subscribe([self.frame_topic])
        # partitioner for processed frame topic
        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.frame_topic, partition=i)
                                                 for i in range(self.topic_partitions)])
        else:
            partitioner = Murmur2Partitioner(partitions=
                                             [TopicPartition(topic=self.frame_topic, partition=i)
                                              for i in range(self.topic_partitions)])
        #  Produces prediction object
        url_producer = KafkaProducer(bootstrap_servers=[params.KAFKA_BROKER],
                                                 key_serializer=lambda key: str(key).encode(),
                                                 value_serializer=lambda value: json.dumps(value).encode(),
                                                 partitioner=partitioner)
        print("started", self.iam)
        try:
            while True:
                # if self.verbose:
                    # print("[ConsumeFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))
                raw_frame_messages = porter_consumer.poll(timeout_ms=10, max_records=10)
                for topic_partition, msgs in raw_frame_messages.items():

                    for msg in msgs:

                        result = self.store_tmp_frame(msg.value)

                        if self.verbose:
                            print(result["frame_num"])
                            print(result['s3_key'])
                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        porter_consumer.commit(offsets=offsets)

                        # Partition to be sent to
                        url_producer.send(self.url_topic,
                                          key="{}_{}".format(result["camera"], result["frame_num"]),
                                          value=result)

                    url_producer.flush()
        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            porter_consumer.close()

    def store_tmp_frame(self, frame_obj):
        """Processes value produced by producer, returns prediction with png image.
        From here, the message don't contain the image
        :param frame_obj: frame dictionary with frame information and the frame
        :return: url_for the tmp file
        """
        # frame_obj = json
        frame = np_from_json(frame_obj, prefix_name=settings.ORIGINAL_PREFIX)
        # Convert the image from BGR color (which OpenCV uses) to RGB color
        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

        cam_id = frame_obj['camera']
        timestamp = frame_obj['timestamp']

        s3_key = self.s3writer.upload_public_delete_local(frame, cam_id, timestamp)

        urldict = {'s3_key': s3_key,
                   'camera': cam_id,
                   'timestamp': timestamp,
                   'frame_num': frame_obj['frame_num'],
                   'location': frame_obj['location']}

        return urldict
