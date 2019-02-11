#!/usr/bin/env python3
# video producers

"""
Simulate real-time video streaming:
1. The source could be a video file.
2. The source could also be a folder contains a sequence of keyframes.
"""
import re
import json
import sys
import time
from multiprocessing import Process

import cv2
import imutils
import numpy as np
from imutils.video import VideoStream

from kafka import KafkaProducer, TopicPartition
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
import src.params as params
from src.kafka.utils import np_to_json
from src.utils import get_curtimestamp_millis
import src.processing.cvutils as cvutils
import src.kafka.settings as settings
from src.processing.sampling_with_location import VideoSampler


class StreamVideo(Process):
    def __init__(self, video_path,
                 topic,
                 topic_partitions=8,
                 use_cv2=False,
                 pub_obj_key=settings.ORIGINAL_PREFIX,
                 sample_speed=10,
                 location='Unknown',
                 group=None,
                 target=None,
                 name=None,
                 verbose=False,
                 rr_distribute=False):
        """Video Streaming Producer Process Class. Publishes frames from a video source to a topic.
        :param video_path: video path or url
        :param topic: kafka topic to publish stamped encoded frames.
        :param topic_partitions: number of partitions this topic has, for distributing messages among partitions
        :param use_cv2: send every frame, using cv2 library, else will use imutils to speedup training
        :param pub_obj_key: associate tag with every frame encoded, can be used later to separate raw frames
        :param sample_speed: to decrease the fps of incoming video
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        :param verbose: print logs on stdout
        :param rr_distribute: use round robin partitioner, should be set same as consumers.
        """
        super().__init__(group=group, target=target, name=name)
        # This is the folder for videos
        self.video_path = video_path
        # TOPIC TO PUBLISH
        self.frame_topic = topic
        self.topic_partitions = topic_partitions
        # Get the camera_num for the steam name
        self.camera_num = int(re.findall(r"StreamVideo-([0-9]*)", self.name)[0])
        self.use_cv2 = use_cv2
        self.object_key = settings.ORIGINAL_PREFIX
        self.verbose = verbose
        self.rr_distribute = rr_distribute
        self.sampler = VideoSampler(sample_speed)

        # For first version, we just use the car's registion location
        # In future, we could use GPS information
        self.timer = time.time()
        self.zerotime = time.time()
        self.sizecnt = 0
        self.location = location

    def run(self):
        """Publish video frames as json objects, timestamped, marked with camera number.
        Source:
            self.video_path: URL for streaming video
            self.kwargs["use_cv2"]: use raw cv2 streaming, set to false to use smart fast streaming --> not every frame is sent.
        Publishes:
            A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """
        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.frame_topic, partition=i)
                                                 for i in range(self.topic_partitions)])
        else:
            partitioner = Murmur2Partitioner(partitions=
                                             [TopicPartition(topic=self.frame_topic, partition=i)
                                              for i in range(self.topic_partitions)])

        # Producer object, set desired partitioner
        frame_producer = KafkaProducer(bootstrap_servers=[params.KAFKA_BROKER],
                                       key_serializer=lambda key: str(key).encode(),
                                       value_serializer=lambda value: json.dumps(value).encode(),
                                       partitioner=partitioner,
                                       max_request_size=134217728)

        print("[CAM {}] URL: {}, SET PARTITIONS FOR FRAME TOPIC: {}".format(self.camera_num,
                                                                            self.video_path,
                                                                            frame_producer.partitions_for(
                                                                                self.frame_topic)))
        # Use either option
        if self.use_cv2:
            # video = cv2.VideoCapture(self.video_path)
            # Here we use sampler to read all videos from a folder
            self.sampler.add_video(self.video_path)
        else:
            video = VideoStream(self.video_path).start()

        # Track frame number
        frame_num = 0
        start_time = time.time()
        print("[CAM {}] START TIME {}: ".format(self.camera_num, start_time))

        while True:
            if self.use_cv2:
                success, image, self.location = self.sampler.read()
                if not success:
                    if self.verbose:
                        print("[CAM {}] URL: {}, END FRAME: {}".format(self.name,
                                                                       self.video_path,
                                                                       frame_num))
                    break
            else:
                image = video.read()
                if image is None:
                    if self.verbose:
                        print("[CAM {}] URL: {}, END FRAME: {}".format(self.name,
                                                                       self.video_path,
                                                                       frame_num))
                    break
            # Attach metadata to frame, transform into JSON
            message = self.transform(frame=image,
                                     frame_num=frame_num,
                                     location=self.location,
                                     object_key=self.object_key,
                                     camera=self.camera_num,
                                     verbose=self.verbose)
            self.sizecnt += sys.getsizeof(message)
            if time.time() - self.timer > 1:
                acc = self.sizecnt
                if self.verbose:
                    print("Time {} send out size {}".format(int(self.timer - self.zerotime), acc))
                    print(self.location)
                self.sizecnt = 0
                self.timer = time.time()


            # Callback function
            def on_send_success(record_metadata):
                print(record_metadata.topic)
                print(record_metadata.partition)
                print(record_metadata.offset)

            def on_send_error(excp):
                print(excp)
                # log.error('I am an errback', exc_info=excp)

            #  Partition to be sent to
            part = frame_num % self.topic_partitions
            # Logging
            # Publish to specific partition
            if self.verbose:
                print("\r[PRODUCER][Cam {}] FRAME: {} TO PARTITION: {}".format(message["camera"], frame_num, part))
                frame_producer.send(self.frame_topic, key="{}_{}".format(self.camera_num, frame_num), value=message).add_callback(on_send_success).add_errback(on_send_error)
            else:
                frame_producer.send(self.frame_topic, key="{}_{}".format(self.camera_num, frame_num), value=message)

            # if frame_num % 1000 == 0:
            frame_producer.flush()

            frame_num += 1

        if self.use_cv2:
            self.sampler.release()
        else:
            video.stop()

        if self.verbose:
            print("[CAM {}] FINISHED. STREAM TIME {}: ".format(self.camera_num, time.time() - start_time))

        return True if frame_num > 0 else False

    @staticmethod
    def transform(frame, frame_num, location, object_key, camera=0, verbose=False):
        """Serialize frame, create json message with serialized frame, camera number and timestamp.
        :param frame: numpy.ndarray, raw frame
        :param frame_num: frame number in the particular video/camera
        :param location: the location of the cars
        :param object_key: identifier for these objects
        :param camera: Camera Number the frame is from
        :param verbose: print out logs
        :return: A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """
        frame = imutils.resize(frame, width=400)
        frame = cvutils.rotate(frame)

        if verbose:
            # print raw frame size
            print("\nRAW ARRAY SIZE: ", sys.getsizeof(frame))

        # serialize frame
        frame_dict = np_to_json(frame.astype(np.uint8), prefix_name=object_key)
        # Metadata for frame
        message = {"timestamp": get_curtimestamp_millis(),
                   "location": location,
                   "camera": camera,
                   "frame_num": frame_num}
        # add frame and metadata related to frame
        message.update(frame_dict)

        if verbose:
            # print message size
            print("\nMESSAGE SIZE: ", sys.getsizeof(message))

        return message
