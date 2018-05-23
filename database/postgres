'''
Created on May 7, 2018

@author: Yaman Noaiseh
'''
# Creates Postgres table for fraudulent transactions 

import psycopg2 as pg

# will be refactored and imported from properties file
CONN_STR = '''
           host=yamandbinstance.cdv4ju31golw.us-east-1.rds.amazonaws.com 
           dbname=dbname 
           user=dbusername 
           password=''
           '''

DROP_TABLE = 'DROP TABLE IF EXISTS fraud_txns;'

CREATE_TABLE =  '''
                CREATE TABLE fraud_txns (
                id SERIAL PRIMARY KEY,
                user_id INT,
                vendor VARCHAR(10) NOT NULL,
                vendor_loc VARCHAR(50) NOT NULL,
                txn_time TIMESTAMP NOT NULL,
                amount NUMERIC NOT NULL,
                distance NUMERIC NOT NULL
                );
                '''

def create_table():
    conn = pg.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute(DROP_TABLE)
    conn.commit()
    cursor.execute(CREATE_TABLE)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_table()
