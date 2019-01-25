import sys
sys.path.append("../")

from processor import YoloDetectron
from kafka import KafkaConsumer
# import cv2
from kafka import KafkaProducer
from multiprocessing import Process


intopic = "compressed_stream"
outtopic = "toshow_video"


class ObjDetectron(Process):
    def __init__(self):
        super().init()
        self.consumer = KafkaConsumer(
            intopic,
            bootstrap_servers=['localhost:9092']
        )
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.detectron = YoloDetectron()
        print("Compressor started.")

    def run(self):
        for msg in self.consumer:
            processed_frame = self.detectron(msg.value)
            self.producer.send(outtopic, processed_frame)


if __name__ == '__main__':
    p = ObjDetectron()
    p.start()
