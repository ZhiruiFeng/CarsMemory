import sys
sys.path.append('../../')

from src.producer.video_producer import StreamVideo
from src.kafka.utils import clear_topic, set_topic


def start_producer(url, id):
    topic = "org_"+str(id)
    name = "StreamVideo-" + str(id)
    partitions = 16
    use_cv2 = True
    verbose = False
    pub_obj_key = "original"
    rr_distribute = False
    producer = StreamVideo(url, topic, partitions,
                           sample_speed=10,
                           use_cv2=use_cv2,
                           verbose=verbose,
                           pub_obj_key=pub_obj_key,
                           rr_distribute=rr_distribute,
                           name=name)
    return producer


if __name__ == "__main__":
    urls = ['http://s3-us-west-2.amazonaws.com/dashcash/dataset/samples-1k/videos/00091078-2bcd1ac9.mov']
    producers = [start_producer(url, i+1) for i, url in enumerate(urls)]
    for p in producers:
        p.start()
    for p in producers:
        p.join()
