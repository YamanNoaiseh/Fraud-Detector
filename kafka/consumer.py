import psycopg2 as pg
from kafka import KafkaConsumer as kc
from datetime import datetime as dt
import json
 
conn = pg.connect("host=host_public_dns password=pw")
cursor = conn.cursor()
cursor.execute('DROP TABLE IF EXISTS previous_locations')
conn.commit()
cursor.execute('''CREATE TABLE previous_locations(
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                time VARCHAR(100) NOT NULL,
                location VARCHAR(100));''')

conn.commit()

consumer = kc('testtopic', bootstrap_servers=['localhost:9092'],group_id = "test",value_deserializer = lambda v : json.loads(v.decode("utf-8")))

def consume_messages():
    for msg in consumer:
        try:
            elements = msg.value.split(',')
            elements = [x.replace( "\"","").replace("\n","") for x in elements]
            time = elements[1]
            location = elements[2].strip()
            statement = "INSERT INTO previous_locations(user_id, time, location) values({},'{}','{}');".format(elements[0],time,location)
            cursor.execute(statement)
            print('inserted line...')
            conn.commit()
        except Exception as e:
            print("Exception {} on offset {}".format(e,msg.offset))

if __name__ == '__main__':
    consume_messages()
