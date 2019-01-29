from src.params import KAFKA_BROKER
from confluent_kafka import Producer


p = Producer({'bootstrap.servers': KAFKA_BROKER})

try:
    lines = "this \n is \n a \n test".split('\n')
    for line in lines:
        p.produce('raw_message', line)
        p.poll(0.5)
except KeyboardInterrupt:
    pass

p.flush(30)
