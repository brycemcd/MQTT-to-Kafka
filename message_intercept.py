import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os, ssl

## KAFKA

# Create a SSL context
# NOTE: copy contents of ~/Downloads/homeauto.sh to shell env prior to running this
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                         cadata=os.environ.get('CLOUDKARAFKA_CA'))

# Write the client cert from the environment variables
# to files on disk and then load them into the context
with open("/tmp/client.pem", "w") as f:
    f.write(os.environ.get('CLOUDKARAFKA_CERT'))
with open("/tmp/client.key", "w") as f:
    f.write(os.environ.get('CLOUDKARAFKA_PRIVATE_KEY'))
ssl_context.load_cert_chain('/tmp/client.pem', '/tmp/client.key')

# Load the rest of the env variables
brokers = os.environ.get('CLOUDKARAFKA_BROKERS').split(',')
topic_prefix = os.environ.get('CLOUDKARAFKA_TOPIC_PREFIX')

producer = KafkaProducer(bootstrap_servers=brokers,
                         security_protocol='SSL',
                         ssl_context=ssl_context)

# Asynchronous by default
def send_message_to_kafka(message):
	producer.send(topic_prefix + 'weather', message)

## MQTT
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("doorjamb")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    send_message_to_kafka(msg.payload)

client = mqtt.Client(client_id="home_connector001")

client.on_connect = on_connect
client.on_message = on_message

client.connect("spark4.thedevranch.net", 1883, 60)

client.loop_forever()
