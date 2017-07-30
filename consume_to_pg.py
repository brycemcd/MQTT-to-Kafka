import kafka_consume as kafka_cons
import psycopg2
import json

## KAFKA
consumer_group = "weather_consumer_pg02"
consumer_device = "weather_consumer004"
kafka_topic = "weather"

consumer = kafka_cons.start_consumer(consumer_group,
                                     consumer_device,
                                     kafka_topic)
## PG
conn=psycopg2.connect(dbname="weather_iot",
                      user="weather_writer",
                      host="psql01.thedevranch.net",
                      # NOTE: add password to pgpass
                      password="",
                      )
cur = conn.cursor()

def write_to_db(message):

    try:
        values = json.loads(message)
        cur.execute(
            """INSERT INTO weather_readings (
                light
                , humidity
                , temp_celcius
                , heat_index
                , capture_dttm
                , pressure_pa
                , baro_temp_celcius
                , device
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""",
            (values['light'],
             values['humidity'],
             values['temp_celcius'],
             values['heat_index'],
             values['capture_dttm'],
             values['pressure_pa'],
             values['baro_temp_celcius'],
             "weather001")
             )

    # This is raised when a unique key contraint exception happens
    except psycopg2.IntegrityError:
        print("record already exists")
    except (json.decoder.JSONDecodeError, KeyError):
        print("message is not JSON or does not contain the right key")
        print(message)
    conn.commit()
    return True

print ('Start consuming')
for message in consumer:
    print(message.value.decode('utf-8'))
    write_to_db(message.value.decode('utf-8'))

cur.close()
conn.close()
