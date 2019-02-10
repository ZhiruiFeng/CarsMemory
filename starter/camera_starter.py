import sys
sys.path.append('../')
from src.producer.video_producer import StreamVideo
from src.kafka.utils import clear_topic, set_topic, topic_is_alive
from src.params import FRAME_PARTITIONS, VALUE_PARTITIONS
from src.consumers.extractor import Extractor


def start_producer(s3_folder_key, id, location):
    topic = "org"
    name = "StreamVideo-" + str(id)
    partitions = FRAME_PARTITIONS
    use_cv2 = True
    verbose = False
    pub_obj_key = "original"
    rr_distribute = False
    producer = StreamVideo(s3_folder_key, topic, location, partitions,
                           sample_speed=5,
                           use_cv2=use_cv2,
                           verbose=verbose,
                           pub_obj_key=pub_obj_key,
                           rr_distribute=rr_distribute,
                           name=name)
    return producer


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage producer_starter.py <dashcam_id> <s3_folder_key> <location>")
        exit(-1)
    cam_id = str(sys.argv[1])
    if str(sys.argv[2])[-1] != '/':
        print("The second parameter should be a s3 foldr key, end with '/'")
        exit(-1)
    s3_folder_key = str(sys.argv[2])

    location = str(sys.argv[3])

    # Set obj_topic and start extractor
    obj_topic = 'obj_' + cam_id
    group_id = 'extractir' + cam_id
    if not topic_is_alive(obj_topic):
        set_topic(obj_topic, partitions=1)
    value_topic = "value"
    extractor = Extractor(obj_topic,
                          value_topic,
                          topic_partitions=VALUE_PARTITIONS,
                          verbose=False,
                          rr_distribute=False,
                          group_id=group_id)
    producer = start_producer(s3_folder_key, cam_id)
    extractor.start()
    producer.start()
    producer.join()
    extractor.join()
