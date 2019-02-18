"""Only need to start once for the whole system"""
import sys
sys.path.append('/home/ubuntu/workspace/CarsMemory/')
from src.kafka.utils import set_topic
from src.params import FRAME_PARTITIONS, URL_PARTITIONS, VALUE_PARTITIONS


if __name__ == "__main__":
    frame_topic = "org"
    frame_partitions = FRAME_PARTITIONS
    url_topic = "url"
    url_partitions = URL_PARTITIONS
    value_topic = "value"
    value_partitions = VALUE_PARTITIONS
    set_topic(frame_topic, partitions=frame_partitions)
    set_topic(url_topic, partitions=url_partitions)
    set_topic(value_topic, partitions=value_partitions)
