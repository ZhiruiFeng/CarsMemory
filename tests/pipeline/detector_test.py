import sys
sys.path.append('../../')

from src.consumers.obj_consumer import ObjConsumer
from src.kafka.utils import clear_topic, set_topic


if __name__ == "__main__":
    frame_topic = "video_test_org_1"
    url_topic = "porter_test_1"
    obj_topic = "obj_test_1"
    group_id = "objconsumer"
    topic_partitions = 16
    verbose = False
    rr_distribute = False
    set_topic(obj_topic, partitions=topic_partitions)
    porter_group = [ObjConsumer(url_topic=url_topic,
                           obj_topic=obj_topic,
                           topic_partitions=topic_partitions,
                           verbose=verbose,
                           rr_distribute=rr_distribute,
                           group_id=group_id) for _ in range(16)]
    for p in porter_group:
        p.start()

    for p in porter_group:
        p.join()
