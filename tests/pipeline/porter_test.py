import sys
sys.path.append('../../')

from src.consumers.porter import Porter
from src.kafka.utils import clear_topic, set_topic


if __name__ == "__main__":
    frame_topic = "video_test_org_1"
    url_topic = "porter_test_1"
    topic_partitions = 16
    verbose = False
    rr_distribute = False
    set_topic(frame_topic, partitions=16)
    set_topic(url_topic, partitions=topic_partitions)
    porter_group = [Porter(frame_topic=frame_topic,
                           url_topic=url_topic,
                           topic_partitions=topic_partitions,
                           verbose=verbose,
                           rr_distribute=rr_distribute,
                           group_id="porter") for _ in range(16)]
    for p in porter_group:
        p.start()

    for p in porter_group:
        p.join()
