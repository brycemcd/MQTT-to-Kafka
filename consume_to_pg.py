import kafka_consume as kafka_cons
from kafka.errors import KafkaError, NoBrokersAvailable
import psycopg2
import json
import os
import time


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
                , mq135
                , mq5
                , mq6
                , mq9
                , device
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
            (values['light'],
             values['humidity'],
             values['temp_celcius'],
             values['heat_index'],
             values['capture_dttm'],
             values['pressure_pa'],
             values['baro_temp_celcius'],
             values['mq135'],
             values['mq5'],
             values['mq6'],
             values['mq9'],
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


if __name__ == '__main__':
    attempts = 0
    max_attempts = os.environ.get('MAX_CONNECTION_RETRIES', 10)

    while attempts < int(max_attempts):
        try:
            ## PG
            # FIXME: these should be env vars
            conn=psycopg2.connect(dbname="weather_iot",
                                  user="weather_writer",
                                  host="psql02.thedevranch.net",
                                  password=os.environ.get('POSTGRES_PASSWD'),
                                  )
            cur = conn.cursor()

            ## KAFKA
            consumer_group = "weather_consumer_pg"
            consumer_device = "weather_consumer_pg_%s" % os.getenv("HOSTNAME",
                                                                   "001")
            kafka_topic = "weather"

            consumer = kafka_cons.start_consumer(consumer_group,
                                                 consumer_device,
                                                 kafka_topic)
            print('Start consuming')
            for message in consumer:
                print(message.value.decode('utf-8'))
                write_to_db(message.value.decode('utf-8'))

            cur.close()
            conn.close()

        except NoBrokersAvailable:
            print("No Brokers. Attempt %s" % attempts)
            attempts = attempts + 1
            time.sleep(2)
