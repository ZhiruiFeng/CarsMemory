import sys
sys.path.append('../../')
from confluent_kafka import Consumer, KafkaError
from src.params import KAFKA_BROKER
from src.cassandra.db_connector import CassandraConnector
import datetime
from src.cassandra.db_writer import DBWriter
import json

settings = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': "sinker",
    'client.id': "client-1",
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)
c.subscribe(['value_test_1'])

dbwriter = DBWriter()

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
            msginfo = msg.value().decode("utf-8")
            msginfo = json.loads(msginfo)
            print(msginfo)
            dbwriter.insert_new_to_frame(msginfo)
            
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
