'''
Created on May 7, 2018

@author: Yaman Noaiseh
'''
# Python consumer for the fraud topic

import json

from kafka import KafkaConsumer
import psycopg2 as pg


class FraudConsumer:

    consumer = KafkaConsumer('fraudtopic', 
                             bootstrap_servers=[
                                 'ec2-35-171-168-73.compute-1.amazonaws.com:9092',
                                 'ec2-34-193-154-78.compute-1.amazonaws.com:9092',
                                 'ec2-18-205-124-122.compute-1.amazonaws.com:9092',
                                 'ec2-18-204-40-198.compute-1.amazonaws.com:9092'
                             ],
                             group_id = '1',
                             value_deserializer = lambda v : json.loads(v.decode('utf-8'))
               )
    
    CONN_STR = '''
               host=yamandbinstance.cdv4ju31golw.us-east-1.rds.amazonaws.com 
               dbname=user_pins 
               user=yamandbmaster 
               password=dataimpactaws
               '''
    
    INSERT_STR = '''
                 INSERT INTO fraud_txns 
                 (user_id, vendor, vendor_loc, txn_time, amount, distance) 
                 values({},'{}','{}','{}',{},{})
                 '''
    
    def consume(self):
        conn = pg.connect(FraudConsumer.CONN_STR)
        cursor = conn.cursor()
        for message in FraudConsumer.consumer:
            print('Got a message!')
            # fraud_msg = user, vendor, vendor-loc, txn-time, txm-amount, distance
            msg = message.value[1:-1]
            elements = msg.split(',')
            statement = FraudConsumer.INSERT_STR.format(elements[0], elements[1], 
                                                        elements[2], elements[3],
                                                        elements[4], elements[5]) 
            cursor.execute(statement)
            conn.commit()
            
if __name__ == '__main__':
    cons = FraudConsumer()
    cons.consume()
