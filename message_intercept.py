import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import time
import os


## KAFKA
def send_message_to_kafka(message):
    """
    Sends message to kafka (duh). Async by default.
    :param message:
    :return:
    """

    print("sending message to kafka: %s" % message)
    producer.send('weather', message)


## MQTT
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    """
    Callback for connect to MQTT event

    :param client:
    :param userdata:
    :param flags:
    :param rc:
    :return: None
    """

    print("Connected %s, %s, %s %s" % (client, userdata, flags, rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("weather")

def on_disconnect(client, user_data, rc):
    """
    Callback for disconnect event

    :param client:
    :param user_data:
    :param rc:
    :return: None
    """

    print("""Disconnected
    client: %s
    user_data: %s
    rc: %s
    """ % (client, user_data, rc))


def on_message(client, userdata, msg):
    """
    The callback for when a PUBLISH message is received from the server.

    :param client:
    :param userdata:
    :param msg:
    :return: None
    """
    print(msg.topic+" "+str(msg.payload))
    send_message_to_kafka(msg.payload)


def mqtt_to_kafka_run():
    """Pick messages off MQTT queue and put them on Kafka"""

    client_name = "home_connector_%s" % os.getenv("HOSTNAME")
    client = mqtt.Client(client_id=client_name)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # TODO: the queue address should be an env var
    client.connect("weathermessaging_mqtt_1", 1883, 60)

    client.loop_forever()


if __name__ == '__main__':
    attempts = 0

    while attempts < 10:
        try:
            brokers = os.getenv("KAFKA_HOSTS", "").split(",")
            producer = KafkaProducer(bootstrap_servers=brokers)
            mqtt_to_kafka_run()

        # NOTE: this is a KAFKA error
        except NoBrokersAvailable:
            print("No Brokers. Attempt %s" % attempts)
            attempts = attempts + 1
            time.sleep(2)

