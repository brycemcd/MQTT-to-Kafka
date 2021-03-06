# Kafka Junction

## What

The purpose of this repo is to serve as an intermediary between the MQTT based
message queue and a Kafka cluster.

## Why

The MQTT is optimized for low overhead for message passing between IoT devices
(low power, low processor, memory specs). It's very possible for multiple consumers
to consume an MQTT queue, but:

* history is lost if a consumer is not connected to the queue and a publisher
  pushes a message
* library support for MQTT is not very popular
* Creating a HA cluster of MQTT brokers is not well supported without a commercial
  license

I don't really like this architecture, but I want to be able to stop the consumer
from time to time for development and then I want it to "catch up" with the messages
it missed during development.

## How to start

NOTE: this is deployed via the `weather-messaging` project in my
home-datacenter-ansible project.

### Env Vars

Populate .env in the root directory. Copy from the sample env.sample
file for a list of required keys.

## Consumers/Producers

### MQTT -> Kafka Bridge

Connect the MQTT -> Kafka bridge with this command: `python message_intercept.py`
This will get messages pushed on the MQTT queue read and pushed on to the Kafka
broker.

### Elasticsearch

Read the messages on the kafka topic and push them to elasticsearch with this:
`python consume_to_es.py`

To create another "sync" to send data to, create a new file and follow the pattern
in consume_to_es.py

### Cloudwatch

`consume_to_cloudwatch.py` is a consumer of the weather data that pushes details
of the event collection to Amazon CloudWatch. To start that consumer,
make sure that the kafka env vars have been set in that console's
session. Then, make sure the following env vars are set:

`AWS_ACCESS_KEY_ID`

`AWS_SECRET_ACCESS_KEY`

`AWS_DEFAULT_REGION` (us-west-2) is where they're being stored now

Then, run the following script:

`python consume_to_cloudwatch.py`

Events are submitted to AMZN with the `cloudWatchPutter` user
credentials. That user has specific permissions to put cloud watch
metrics.
