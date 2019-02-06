"""Set up topics and all consumers except sinker"""

import sys
sys.path.append('../../')
from src.kafka.utils import set_topic
from src.consumers.porter import Porter
from src.consumers.obj_consumer import ObjConsumer
from src.consumers.extractor import Extractor
from src.consumers.librarian import Librarian


def start_topics():
    set_topic(frame_topic, partitions=frame_partitions)
    set_topic(url_topic, partitions=url_partitions)
    set_topic(obj_topic, partitions=obj_partitions)
    set_topic(value_topic, partitions=value_partitions)

def start_consumers():
    porter_group = [Porter(frame_topic=frame_topic,
                           url_topic=url_topic,
                           topic_partitions=url_partitions,
                           verbose=False,
                           rr_distribute=False,
                           group_id="porter"+str(cam_id)) for _ in range(frame_partitions)]
    detector_group = [ObjConsumer(url_topic=url_topic,
                           obj_topic=obj_topic,
                           topic_partitions=obj_partitions,
                           verbose=False,
                           rr_distribute=False,
                           group_id="detector"+str(cam_id)) for _ in range(url_partitions)]
    extractor = Extractor(meta_topic=obj_topic,
                           value_topic=value_topic,
                           topic_partitions=value_partitions,
                           verbose=False,
                           rr_distribute=False,
                           group_id="extractor"+str(cam_id))
    librarian = Librarian(value_topic=value_topic,
                           verbose=False,
                           rr_distribute=False,
                           group_id="librarian")
    librarian.start()
    extractor.start()
    for p in detector_group:
        p.start()
    for p in porter_group:
        p.start()
    for p in porter_group:
        p.join()
    for p in detector_group:
        p.join()
    librarian.join()
    extractor.join()


if __name__ == "__main__":
    cam_id = 1
    frame_topic = "org_" + str(cam_id)
    frame_partitions = 8
    url_topic = "url_" + str(cam_id)
    url_partitions = 16
    obj_topic = "obj_" + str(cam_id)
    obj_partitions = 1
    # Till now, the computational tense module passsed.
    # Thus we opnly need one partition, and this is for aggregation
    value_topic = "value"
    value_partitions = 1
    # For librarian and db_sinker, they don't need to different
    # camera data source, so just share one partition.
    start_topics()
    start_consumers()
