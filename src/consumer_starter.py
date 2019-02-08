"""Need to start seperately on each processing node"""
from src.consumers.porter import Porter
from src.consumers.obj_consumer import ObjConsumer
from src.consumers.librarian import Librarian
from src.params import FRAME_PARTITIONS, URL_PARTITIONS, VALUE_PARTITIONS


frame_topic = "org"
frame_partitions = FRAME_PARTITIONS
url_topic = "url"
url_partitions = URL_PARTITIONS
value_topic = "value"
value_partitions = VALUE_PARTITIONS


if __name__ == "__main__":
    porter_group = [Porter(frame_topic=frame_topic,
                           url_topic=url_topic,
                           topic_partitions=url_partitions,
                           verbose=False,
                           rr_distribute=False,
                           group_id="porter") for _ in range(int(frame_partitions/4))]
    # obj_topic here is a prefix.
    detector_group = [ObjConsumer(url_topic=url_topic,
                           obj_topic="obj",
                           topic_partitions=1,
                           verbose=False,
                           rr_distribute=False,
                           group_id="detector") for _ in range(int(url_partitions/4))]

    librarian_group = [Librarian(value_topic=value_topic,
                           verbose=False,
                           rr_distribute=False,
                           group_id="librarian") for _ in range(int(value_partitions/4))]
    for p in librarian_group:
        p.start()
    for p in detector_group:
        p.start()
    for p in porter_group:
        p.start()
    for p in porter_group:
        p.join()
    for p in detector_group:
        p.join()
    for p in librarian_group:
        p.join()
