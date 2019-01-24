import sys
import time
import cv2
from kafka import KafkaProducer


topic = "distributed-video1"


def publish_video(video_file):
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # Open video file
    video = cv2.VideoCapture(video_file)

    print("publishing video...")

    while(video.isOpened()):
        success, frame = video.read()
        if not success:
            print("bad read!")
            break
        ret, buffer = cv2.imencode('.jpg', frame)

        producer.send(topic, buffer.tobytes())
        time.sleep(0.2)
    video.release()
    print('publish complete')


if __name__ == '__main__':
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print("add the video file address")
