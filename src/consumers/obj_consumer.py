"""This type of consumer is for object detection"""

import socket
import json
import time
import cv2
import src.params as params
import src.kafka.settings as settings
from contextlib import contextmanager
from src.processing.objdetector import detect_object
from src.kafka.utils import np_from_json, np_to_json

import numpy as np
from multiprocessing import Process
from kafka import KafkaConsumer, KafkaProducer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition


class ObjConsumer(Process):

    def __init__(self,
                 frame_topic,
                 processed_frame_topic,
                 topic_partitions=8,
                 scale=1.0,
                 verbose=False,
                 rr_distribute=False,
                 group=None,
                 target=None,
                 name=None):
        """
        OBJECT DETECTION IN FRAMES --> Consuming encoded frame messages, detect faces and their encodings [PRE PROCESS],
        publish it to processed_frame_topic where these values are used for face matching with query faces.
        :param frame_topic: kafka topic to consume stamped encoded frames.
        :param processed_frame_topic: kafka topic to publish stamped encoded frames with face detection and encodings.
        :param topic_partitions: number of partitions processed_frame_topic topic has, for distributing messages among partitions
        :param scale: (0, 1] scale image before face recognition, but less accurate, trade off!!
        :param verbose: print logs on stdout
        :param rr_distribute:  use round robin partitioner and assignor, should be set same as respective producers or consumers.
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        """

        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = frame_topic

        self.verbose = verbose
        self.scale = scale
        self.topic_partitions = topic_partitions
        self.processed_frame_topic = processed_frame_topic
        self.rr_distribute = rr_distribute
        print("[INFO] I am ", self.iam)

    def run(self):
        """Consume raw frames, detects faces, finds their encoding [PRE PROCESS],
           predictions Published to processed_frame_topic fro face matching."""

        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        frame_consumer = KafkaConsumer(group_id="consume", client_id=self.iam,
                                       bootstrap_servers=[params.KAFKA_BROKER],
                                       key_deserializer=lambda key: key.decode(),
                                       value_deserializer=lambda value: json.loads(value.decode()),
                                       partition_assignment_strategy=partition_assignment_strategy,
                                       auto_offset_reset="earliest")

        frame_consumer.subscribe([self.frame_topic])

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
        processed_frame_producer = KafkaProducer(bootstrap_servers=[params.KAFKA_BROKER],
                                                 key_serializer=lambda key: str(key).encode(),
                                                 value_serializer=lambda value: json.dumps(value).encode(),
                                                 partitioner=partitioner)
        try:
            while True:

                if self.verbose:
                    print("[ConsumeFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))

                raw_frame_messages = frame_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partition, msgs in raw_frame_messages.items():

                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        # get pre processing result
                        result = self.get_processed_frame_object(msg.value, self.scale)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        frame_consumer.commit(offsets=offsets)

                        # Partition to be sent to
                        processed_frame_producer.send(self.processed_frame_topic,
                                                      key="{}_{}".format(result["camera"], result["frame_num"]),
                                                      value=result)

                    processed_frame_producer.flush()

        except KeyboardInterrupt as e:
            print(e)
            pass

        finally:
            print("Closing Stream")
            frame_consumer.close()

    @staticmethod
    def get_processed_frame_object(frame_obj, scale=1.0):
        """Processes value produced by producer, returns prediction with png image.
        :param frame_obj: frame dictionary with frame information and frame itself
        :param scale: (0, 1] scale image before face recognition, speeds up processing, decreases accuracy
        :return: A dict updated with faces found in that frame, i.e. their location and encoding.
        """

        frame = np_from_json(frame_obj, prefix_name=settings.ORIGINAL_PREFIX)  # frame_obj = json
        # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

        if scale != 1:
            # Resize frame of video to scale size for faster face recognition processing
            rgb_small_frame = cv2.resize(frame, (0, 0), fx=scale, fy=scale)

        else:
            rgb_small_frame = frame

        with timer("PROCESS RAW FRAME {}".format(frame_obj["frame_num"])):
            # Find all the faces and face encodings in the current frame of video
            with timer("Locations in frame"):
                return
                # face_locations = np.array(face_recognition.face_locations(rgb_small_frame))
                # face_locations_dict = np_to_json(face_locations, prefix_name="face_locations")

            with timer("Encodings in frame"):
                return
                # face_encodings = np.array(face_recognition.face_encodings(rgb_small_frame, face_locations))
                #face_encodings_dict = np_to_json(face_encodings, prefix_name="face_encodings")

        # frame_obj.update(face_locations_dict)
        # frame_obj.update(face_encodings_dict)

        return frame_obj

@contextmanager
def timer(name):
    """Util function: Logs the time."""
    t0 = time.time()
    yield
    print("[{}] done in {:.3f} s".format(name, time.time() - t0))
