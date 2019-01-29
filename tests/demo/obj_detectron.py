import sys
sys.path.append("../")

from processor.detectron_yolo import YoloDetectron
from kafka import KafkaConsumer
# import cv2
from kafka import KafkaProducer
from multiprocessing import Process


intopic = "raw_video_stream"
outtopic = "toshow_video"


class ObjDetectron(Process):
    def __init__(self):
        super().__init__()
        self.consumer = KafkaConsumer(
            intopic,
            bootstrap_servers=['localhost:9092']
        )
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.detectron = YoloDetectron()
        print("Detection started.")

    def run(self):
        cnt = 0
        for msg in self.consumer:
            if cnt % 10 == 0:
                processed_frame = self.detectron.processing(msg.value)
                self.producer.send(outtopic, processed_frame)
                print("new frames")


if __name__ == '__main__':
    p = ObjDetectron()
    p.start()
