import sys
sys.path.append('/home/ubuntu/workspace/CarsMemory/')
from src.kafka.utils import clear_topic, set_topic, topic_is_alive
from src.params import VALUE_PARTITIONS
from src.consumers.extractor import Extractor


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage extractor_starter.py <dashcam_id>")
        exit(-1)
    cam_id = str(sys.argv[1])
    # Set obj_topic and start extractor
    obj_topic = 'obj_' + cam_id
    group_id = 'extractor' + cam_id
    # For stress tests, reset the topic
    set_topic(obj_topic, partitions=1)
    value_topic = "value"
    extractor = Extractor(obj_topic,
                          value_topic,
                          topic_partitions=VALUE_PARTITIONS,
                          verbose=False,
                          rr_distribute=False,
                          group_id=group_id)
    extractor.start()
    extractor.join()
