import sys
sys.path.append('../../')
from confluent_kafka import Producer, Consumer, KafkaError
from src.params import KAFKA_BROKER

settings = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': "test_listen",
    'client.id': "client-1",
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)
# p = Producer({'bootstrap.servers': KAFKA_BROKER})

c.subscribe(['video_test_org'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(len(msg.value())))
            # p.produce('first_process', msg.value())
            # p.poll(0.1)
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
    # p.flush(30)
