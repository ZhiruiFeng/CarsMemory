from confluent_kafka import Consumer, KafkaError
from src.params import KAFKA_BROKER
from src.cassandra.db_connector import CassandraConnector
import time


settings = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': "test_firstgroup",
    'client.id': "client-1",
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)
c.subscribe[['first_process']]

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
            db_session = CassandraConnector().session
            current_time = time.time()
            db_session.execute("INSERT INTO message (store_time, test) values(%s, %s)",
                               [current_time, msg.value()])
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
