import sys
sys.path.append('../../')

from src.consumers.librarian import Librarian
from src.kafka.utils import clear_topic, set_topic


if __name__ == "__main__":
    frame_topic = "video_test_org_1"
    url_topic = "porter_test_1"
    obj_topic = "obj_test_1"
    value_topic = 'value_test_1'
    group_id = "librarian"
    topic_partitions = 2
    verbose = True
    rr_distribute = False
    porter_group = [Librarian(value_topic=value_topic,
                           topic_partitions=topic_partitions,
                           verbose=verbose,
                           rr_distribute=rr_distribute,
                           group_id=group_id) for _ in range(2)]
    for p in porter_group:
        p.start()

    for p in porter_group:
        p.join()
