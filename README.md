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

My home workstation has a conda environment set up: `source activate weather`

Export kafka environment variables to the shell

Connect the MQTT -> Kafka bridge with this command: `python message_intercept.py`
This will get messages pushed on the MQTT queue read and pushed on to the Kafka
broker.

Read the messages on the kafka topic and push them to elasticsearch with this:
`python consume_to_es.py`

To create another "sync" to send data to, create a new file and follow the pattern
in consume_to_es.py
