#
import psycopg2 as pg
from datetime import datetime as dt
 
conn = pg.connect("host=yamandbinstance.cdv4ju31golw.us-east-1.rds.amazonaws
  
def stream_transactions():
     cursor = conn.cursor()
          with open('transactions.csv', 'r') as f:
          f.readline()
          for line in f:
              data = line.split(',')
              time = data[1]
             uid = data[2]
             location = data[4].strip()
             statement = '''SELECT * FROM previous_locations
             WHERE user_id={}
             AND time='{}'
             AND location LIKE '%{}%'
             '''.format(uid, time, location)
             cursor.execute(statement)
             all = cursor.fetchall()
             if len(all) == 0:
                 print('suspecious fraud... ' + line)

if __name__ == '__main__':
     stream_transactions()
