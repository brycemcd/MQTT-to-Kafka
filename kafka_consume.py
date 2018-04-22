from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os, ssl
import copy


## KAFKA
def start_consumer(group_id, consumer_id, topic, **kwargs):

    brokers = os.getenv("KAFKA_HOSTS", "").split(",")
    default_config = {
        'auto_offset_reset' : 'earliest',
        'enable_auto_commit' : False,
        'security_protocol' : 'PLAINTEXT', # TODO: provide TLS
    }

    config = copy.copy(default_config)
    for key in config:
        if key in kwargs:
            config[key] = kwargs.pop(key)

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(topic,
                             group_id=group_id,
                             client_id=consumer_id,
                             auto_offset_reset=config['auto_offset_reset'],
                             enable_auto_commit=config['enable_auto_commit'],
                             bootstrap_servers=brokers,
                             security_protocol=config['security_protocol'],
                             )
    return consumer
