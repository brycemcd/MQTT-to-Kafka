import kafka_consume as kafka_cons
import psycopg2
import json

## KAFKA
consumer_group = "weather_consumer_pg"
consumer_device = "weather_consumer002"
kafka_topic = "weather"

consumer = kafka_cons.start_consumer(consumer_group,
                                     consumer_device,
                                     kafka_topic)
## PG
conn=psycopg2.connect(dbname="weather_iot",
                      user="brycemcd",
                      host="spark3.thedevranch.net",
                      # NOTE: add password to pgpass
                      password="",
                      )
cur = conn.cursor()

def write_to_db(message):
    values = json.loads(message)

    try:
        cur.execute(
            """INSERT INTO weather_readings (
                light
                , humidity
                , temp_celcius
                , heat_index
                , capture_dttm
                , device
            ) VALUES (%s, %s, %s, %s, %s, %s);""",
            (values['light'],
             values['humidity'],
             values['temp_celcius'],
             values['heat_index'],
             values['capture_dttm'],
             "weather001")
             )
        conn.commit()
    except KeyError:
        print("message does not contain the right key")
        print(message)

print ('Start consuming')
for message in consumer:
    write_to_db(message.value.decode('utf-8'))
    print(message.value.decode('utf-8'))

cur.close()
conn.close()
