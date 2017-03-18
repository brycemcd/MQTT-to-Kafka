from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os, ssl

## KAFKA
def start_consumer(group_id, consumer_id, topic):
    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
    cadata=os.environ.get('CLOUDKARAFKA_CA'))

    # Write the client cert from the environment variables
    # to files on disk and then load them into the context
    with open("/tmp/client.pem", "w") as f:
        f.write(os.environ.get('CLOUDKARAFKA_CERT'))
    with open("/tmp/client.key", "w") as f:
        f.write(os.environ.get('CLOUDKARAFKA_PRIVATE_KEY'))

    ssl_context.load_cert_chain('/tmp/client.pem', '/tmp/client.key')

    brokers = os.environ.get('CLOUDKARAFKA_BROKERS').split(',')
    topic_prefix = os.environ.get('CLOUDKARAFKA_TOPIC_PREFIX')

    # To consume latest messages and auto-commit offsets
    return KafkaConsumer(topic_prefix + topic,
            group_id=group_id,
            client_id=consumer_id,
            bootstrap_servers=brokers,
            security_protocol='SSL',
            ssl_context=ssl_context)
