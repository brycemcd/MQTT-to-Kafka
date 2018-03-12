import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import os, ssl
import time

## KAFKA

# brokers = os.environ.get('CLOUDKARAFKA_BROKERS').split(',')


# Asynchronous by default
def send_message_to_kafka(message):
    # TODO: this should be an env var
	producer.send('weather', message)

## MQTT
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("weather")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    send_message_to_kafka(msg.payload)

def mqtt_to_kafka_run():
    """Pick messages off MQTT queue and put them on Kafka"""

    client = mqtt.Client(client_id="home_connector001")

    client.on_connect = on_connect
    client.on_message = on_message

    # TODO: this should be an env var
    client.connect("mqtt01.thedevranch.net", 1883, 60)

    client.loop_forever()

if __name__ == '__main__':
    attempts = 0

    while(attempts < 10):
        try:
            brokers = os.environ.get('KAFKA_ADVERTISED_HOST_NAME')
            producer = KafkaProducer(bootstrap_servers=brokers)
            mqtt_to_kafka_run()
        # NOTE: this is 
        except NoBrokersAvailable:
            print("No Brokers. Attempt %s" % attempts)
            attempts = attempts + 1
            time.sleep(2)

