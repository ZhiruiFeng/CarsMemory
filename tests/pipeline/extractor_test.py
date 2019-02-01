import sys
sys.path.append('../../')

from src.consumers.extractor import Extractor
from src.kafka.utils import clear_topic, set_topic


if __name__ == "__main__":
    frame_topic = "video_test_org_1"
    url_topic = "porter_test_1"
    obj_topic = "obj_test_1"
    value_topic = 'value_test_1'
    group_id = "extractor"
    topic_partitions = 2
    verbose = True
    rr_distribute = False
    set_topic(value_topic, partitions=topic_partitions)
    porter_group = [Extractor(meta_topic=obj_topic,
                           value_topic=value_topic,
                           topic_partitions=topic_partitions,
                           verbose=verbose,
                           rr_distribute=rr_distribute,
                           group_id=group_id) for _ in range(2)]
    for p in porter_group:
        p.start()

    for p in porter_group:
        p.join()
