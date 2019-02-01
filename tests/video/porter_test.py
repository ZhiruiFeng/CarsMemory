import sys
sys.path.append('../../')

from src.consumers.porter import Porter
from src.kafka.utils import clear_topic, set_topic


if __name__ == "__main__":
    frame_topic = "video_test_org"
    url_topic = "porter_test"
    topic_partitions = 8
    verbose = False
    rr_distribute = False
    set_topic("video_test_org", partitions=16)
    set_topic(url_topic, partitions=topic_partitions)
    porter_group = [Porter(frame_topic=frame_topic,
                           url_topic=url_topic,
                           topic_partitions=topic_partitions,
                           verbose=verbose,
                           rr_distribute=rr_distribute,
                           group_id="porter") for _ in range(4)]
    for p in porter_group:
        p.start()

    for p in porter_group:
        p.join()
