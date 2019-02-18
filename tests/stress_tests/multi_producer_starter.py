import sys
sys.path.append('/home/ubuntu/workspace/CarsMemory/')
from src.producer.video_producer import StreamVideo
from src.kafka.utils import clear_topic, set_topic
from src.params import FRAME_PARTITIONS
from src.consumers.extractor import Extractor


def start_producer(s3_folder_key, id):
    topic = "org"
    name = "StreamVideo-" + str(id)
    partitions = FRAME_PARTITIONS
    use_cv2 = True
    verbose = False
    pub_obj_key = "original"
    rr_distribute = False
    producer = StreamVideo(s3_folder_key, topic, partitions,
                           sample_speed=5,
                           use_cv2=use_cv2,
                           verbose=verbose,
                           pub_obj_key=pub_obj_key,
                           rr_distribute=rr_distribute,
                           name=name)
    return producer


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage producer_starter.py <group>")
        exit(-1)
    groups = int(sys.argv[1])
    start = groups * 4
    producers = []
    for i in range(4):
        cam_id = start + i
        s3_folder_key = "dataset/dashcam/user_" + str(cam_id) + '/videos'
        producer = start_producer(s3_folder_key, cam_id)
        producers.append(producer)

    for p in producers:
        p.start()
    for p in producers:
        p.join()
