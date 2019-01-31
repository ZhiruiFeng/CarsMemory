"""Parameters Used in Kafka Setting"""


"""Basic"""
ZOOKEEPER_PORT = '2181'
KAFKA_BIN = "/usr/local/kafka/bin/"

"""Topic name and Prefix"""


"""Performance Parameters"""
# USE RAW CV2 STREAMING or FAST BUT LESS FRAMES
USE_RAW_CV2_STREAMING = False
# TOPIC PARTITIONS
SET_PARTITIONS = 16
# PARTITIONER
ROUND_ROBIN = False
