import sys
sys.path.append('../../')

from src.producer.video_producer import StreamVideo
from src.kafka.utils import clear_topic, set_topic

if __name__ == "__main__":
    url = 'http://s3-us-west-2.amazonaws.com/dashcash/dataset/train-14/videos/c4dab4e2-6c4805a2.mov'
    topic = "video_test_org"
    partitions = 16
    use_cv2 = True
    verbose = False
    pub_obj_key = "test"
    rr_distribute = False
    clear_topic(topic)
    set_topic(topic)
    producer = StreamVideo(url, topic, partitions,
                           use_cv2=use_cv2,
                           verbose=verbose,
                           pub_obj_key=pub_obj_key, 
                           rr_distribute=rr_distribute)
    producer.start()