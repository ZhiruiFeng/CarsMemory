# A simple sampling to decrease FPS
# import datetime
from kafka import KafkaConsumer
# import cv2
from kafka import KafkaProducer
from multiprocessing import Process


intopic = "raw_video_stream"
outtopic = "compressed_stream"


class VideoCompressor(Process):
    def __init__(self):
        super().__init__()
        self.consumer = KafkaConsumer(
            intopic,
            bootstrap_servers=['localhost:9092']
        )
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        print("Compressor started.")

    def run(self):
        cnt = 0
        for msg in self.consumer:
            if cnt % 10 == 0:
                self.producer.send(outtopic, msg.value)
                print('new frame')
            cnt += 1


if __name__ == '__main__':
    p = VideoCompressor()
    p.start()
