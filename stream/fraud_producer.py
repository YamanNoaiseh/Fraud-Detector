'''
Created on May 6, 2018

@author: Yaman Noaiseh
'''
# A Kafka producer for detected fraud messages

import json

from kafka.producer import KafkaProducer


class FraudProducer:
    
    producer = KafkaProducer(bootstrap_servers=['ec2-35-171-168-73.compute-1.amazonaws.com:9092',
                                                'ec2-34-193-154-78.compute-1.amazonaws.com:9092',
                                                'ec2-18-205-124-122.compute-1.amazonaws.com:9092',
                                                'ec2-18-204-40-198.compute-1.amazonaws.com:9092'
                                               ],
                             value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    
    def __init__(self, fraud_topic):
        self.fraud_topic = fraud_topic
    
    def produce(self, message):
        message = json.dumps(message)
        FraudProducer.producer.send(self.fraud_topic, message)
        FraudProducer.producer.flush()
