# Kafka python producer
 
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer as kp
import json
 
producer = kp(bootstrap_servers=['localhost:9092'],value_serializer = lambda v: json.dumps(v).encode("utf-8"))

def produce_messages():
    with open('locations.csv', 'r') as f:
        first = True
        for line in f:
            if not first:
                line = json.dumps(line)
                line.strip()
                print("produced {}".format(line))
                producer.send('testtopic', line)
                producer.flush()
            else:
                first = False

if __name__ == '__main__':
    produce_messages()
