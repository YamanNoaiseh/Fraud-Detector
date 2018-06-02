'''
Created on May 7, 2018

@author: Yaman Noaiseh
'''
# Kafka consumer for the transaction topic.
# Inserts incoming transactions to the Redis cluster.

import json

from kafka import KafkaConsumer
import redis


class TransactionConsumer:

    consumer = KafkaConsumer('transaction01', 
                             bootstrap_servers=[
                                 'ec2-35-171-168-73.compute-1.amazonaws.com:9092',
                                 'ec2-34-193-154-78.compute-1.amazonaws.com:9092',
                                 'ec2-18-205-124-122.compute-1.amazonaws.com:9092',
                                 'ec2-18-204-40-198.compute-1.amazonaws.com:9092'
                             ],
                             group_id = '1',
                             value_deserializer = lambda v : json.loads(v.decode('utf-8')))

    def __init__(self):
        self.master_index = 0
        self.redis1 = redis.Redis(host='ec2-18-205-124-122.compute-1.amazonaws.com', 
                            port=6379, charset="utf-8", decode_responses=True, db=0)

        self.redis2 = redis.Redis(host='ec2-18-204-40-198.compute-1.amazonaws.com', 
                            port=6379, charset="utf-8", decode_responses=True, db=0)

    def consume(self):
        print('Transaction Consumer has STARTED')
        for message in TransactionConsumer.consumer:
            # transaction message: merchant_id, user_id, time, amount, latitude, longitude
            msg = message.value[1:-1]
            elements = msg.split(',')
            user = elements[1]
            trans = {'vendor':elements[0], 'time':elements[2], 'amount':elements[3],
                         'lat':elements[4], 'long':elements[5]}
            # add the following Redis hash:
            # key: user_id, value: {vendor:merchant_id, time:timestamp, amount:amount, lat:latitude, long:longitude}
            try:
                self.redis1.hmset(user, trans)
            except Exception:
                pass

            try:
                self.redis2.hmset(user, trans)
            except Exception:
                pass


if __name__ == '__main__':
    cons = TransactionConsumer()
    cons.consume()
