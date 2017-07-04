from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os, ssl
import copy

## KAFKA
def start_consumer(group_id, consumer_id, topic, **kwargs):
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

    default_config = {
        'auto_offset_reset' : 'earliest',
        'enable_auto_commit' : False,
        'security_protocol' : 'SSL',
    }

    config = copy.copy(default_config)
    for key in config:
        if key in kwargs:
            config[key] = kwargs.pop(key)

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(topic_prefix + topic,
                                group_id=group_id,
                                client_id=consumer_id,
                                auto_offset_reset=config['auto_offset_reset'],
                                enable_auto_commit=config['enable_auto_commit'],
                                bootstrap_servers=brokers,
                                security_protocol=config['security_protocol'],
                                ssl_context=ssl_context)
    return consumer
