# A simple sampling to decrease FPS
import datetime
from kafka import KafkaConsumer
import sys
import cv2
from kafka import KafkaProducer


intopic = "raw_video_stream"
outtopic = "compressed_stream"
