'''
Created on May 6, 2018

@author: Yaman Noaiseh
'''
# Stream generator. Generates a transaction and a user locations.
# Generates a fraudulent transaction at the probability of 0.15%.

from datetime import datetime
import json

from faker import Faker
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import random as rd


class MessageProducer:
    
    fake = Faker()
    
    producer = KafkaProducer(bootstrap_servers=['ec2-35-171-168-73.compute-1.amazonaws.com:9092',
                                                'ec2-34-193-154-78.compute-1.amazonaws.com:9092',
                                                'ec2-18-205-124-122.compute-1.amazonaws.com:9092',
                                                'ec2-18-204-40-198.compute-1.amazonaws.com:9092'
                                               ],
                             value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    
    def __init__(self, loc_topic, txn_topic):
        self.loc_topic = loc_topic
        self.txn_topic = txn_topic
    
    def produce_messages(self):
        # location message: user_id, timestamp, latitude, longitude
        # transaction message: merchant_id, user_id, timestamp, amount, latitude, longitude
        while (True):
            # generate a transaction
            mid = 'M' + str(rd.randint(10000, 60000))
            uid = str(rd.randint(1000000, 3999999))
            time = self.get_time()
            amount = str(round(rd.uniform(1, 500), 2))
            latitude = str(MessageProducer.fake.latitude())
            longitude = str(MessageProducer.fake.longitude())
            txn_message = ','.join((mid, uid, time, amount, latitude, longitude))
            self.produce(self.txn_topic, txn_message)
            # uncomment the following line to generate noise data
            # self.produce_noise()
            # generate a matching user location with probability 0.985
            p = rd.uniform(0, 1)
            if p > 0.015:
                usr_message = ','.join((uid, time, latitude, longitude))
            else:
                fr_latitude = str(MessageProducer.fake.latitude())
                fr_longitude = str(MessageProducer.fake.longitude())
                usr_message = ','.join((uid, time, fr_latitude, fr_longitude))
            self.produce(self.loc_topic, usr_message)
            
    def get_time(self):
        time = datetime.now()
        time = time.strftime('%Y-%m-%d %H:%M:%S')
        return str(time)
    
    def produce(self, topic, message):
        message = json.dumps(message)
        MessageProducer.producer.send(topic, message)
        MessageProducer.producer.flush()
    
    def produce_noise(self):
        noise_size = rd.randint(1, 100)
        for _ in range(noise_size):
            uid = str(rd.randint(1000000, 3999999))
            time = self.get_time()
            latitude = str(MessageProducer.fake.latitude())
            longitude = str(MessageProducer.fake.longitude())
            noise_message = ','.join((uid, time, latitude, longitude))
            self.produce(self.loc_topic, noise_message)

if __name__ == '__main__':
    prod = MessageProducer('location01', 'transaction01')
    prod.produce_messages()
