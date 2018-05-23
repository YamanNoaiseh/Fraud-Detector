'''
Created on May 7, 2018

@author: Yaman Noaiseh
'''
# Kafka consumer for the location topic.
# Matches the user location against an existing transaction.

from datetime import datetime, timedelta
import json
import time

import distancer
from fraud_producer import FraudProducer
from gpxpy import geo
from kafka import KafkaConsumer
import redis


class LocationConsumer:
    
    consumer = KafkaConsumer('locationtopic', 
                             bootstrap_servers=[
                                 'ec2-35-171-168-73.compute-1.amazonaws.com:9092',
                                 'ec2-34-193-154-78.compute-1.amazonaws.com:9092',
                                 'ec2-18-205-124-122.compute-1.amazonaws.com:9092',
                                 'ec2-18-204-40-198.compute-1.amazonaws.com:9092'
                             ],
                             group_id = '1',
                             value_deserializer = lambda v : json.loads(v.decode('utf-8'))
               )

    redis1 = redis.Redis(host='ec2-18-204-40-198.compute-1.amazonaws.com', 
                port=6379, charset="utf-8", decode_responses=True, db=0)
    
    redis2 = redis.Redis(host='ec2-18-205-124-122.compute-1.amazonaws.com', 
                port=6379, charset="utf-8", decode_responses=True, db=0)
    redis_pool = (redis1, redis2)
    rpool_size = len(redis_pool)

    def __init__(self):
        self.rmaster_index = 0
        self.rmaster = LocationConsumer.redis_pool[self.rmaster_index]
        self.rbackup = LocationConsumer.redis_pool[self.rmaster_index + 1]
        self.redis_backup_ready = True
        self.fproducer = FraudProducer('fraudtopic')
    
    def consume(self):
        #count = 0
        for message in LocationConsumer.consumer:
            # location msg: uid, time, lat, long
            msg = message.value[1:-1]
            elements = msg.split(',')
            user = elements[0]
            
            try:
                txn_details = self.rmaster.hgetall(user)
            except Exception:
                crash_time = datetime.now()
                self.switch_redis_connection()
                txn_details = self.rmaster.hgetall(user)
                self.redis_backup_ready = False
            
            if txn_details != {}:
                self.validate_transaction(elements, txn_details)
            else:
                print('Cannot be validated')
            
            try:
                res1 = self.rmaster.delete(user)
                if res1 == 0:
                    print('------------LOC - Did not Delete------------ 1')
            except Exception:
                crash_time = datetime.now()
                self.switch_redis_connection()
                self.rmaster.delete(user)
                self.redis_backup_ready = False
            
            if self.redis_backup_ready:
                res = self.rbackup.delete(user)
                if res == 0:
                    print('------------LOC - Did not Delete------------ 2')
            # else: try to reconnect every 5 seconds
            elif (datetime.now() - crash_time).total_seconds() > 5:
                try:
                    self.rbackup.delete(user)
                    print('Successfully reconnected to the backup Redis server...')
                    self.redis_backup_ready = True
                except Exception:
                    # Redis backup server is still out of reach. Update the crash time
                    crash_time = datetime.now()
                    print('Failed attempt to reconnect to the backup Redis server...')
    
    def validate_transaction(self, loc_elements, txn_details):
        user = loc_elements[0]
        loc_time = loc_elements[1]
        latitude = loc_elements[2]
        longitude = loc_elements[3]
        distance = geo.haversine_distance(float(latitude), float(longitude), 
                                          float(txn_details['lat']), float(txn_details['long']))
        txn_time = txn_details['time']
        threashold = distancer.distance_threshold(txn_time, loc_time)
        if threashold != False and distance > threashold:
            print('Suspicious transaction detected...')
            fraud_msg = user \
                        + ',' + txn_details['vendor'] \
                        + ',' + txn_details['lat'] + ';' + txn_details['long'] \
                        + ',' + txn_details['time'] \
                        + ',' + txn_details['amount'] \
                        + ',' + str(distance)                    
            self.fproducer.produce(fraud_msg)
    
    def switch_redis_connection(self):
        print('Switching to an alternative Redis Server..')
        self.rmaster_index = (self.rmaster_index + 1) % LocationConsumer.rpool_size
        self.rbackup_index = (self.rmaster_index + 1) % LocationConsumer.rpool_size
        self.rmaster = LocationConsumer.redis_pool[self.rmaster_index]
        self.rbackup = LocationConsumer.redis_pool[self.rbackup_index]
    
if __name__ == '__main__':
    cons = LocationConsumer()
    cons.consume()
