import kafka_consume as kafka_cons
from elasticsearch import Elasticsearch

## KAFKA
consumer_group = "weather_consumer"
consumer_device = "weather_consumer001"
kafka_topic = "weather"
consumer = kafka_cons.start_consumer(consumer_group,
                                     consumer_device,
                                     kafka_topic)

## ES
es_hosts = [
	'http://web02.thedevranch.net',
	'http://spark3.thedevranch.net',
]
es_index = 'doorjamb'
es_type  = 'door001'
es = Elasticsearch(es_hosts)

def index_message_in_es(message):
	es.index(index=es_index,
             doc_type=es_type,
             body=message)

print ('Start consuming')
for message in consumer:
    index_message_in_es(message.value.decode('utf-8'))
    print(message.value.decode('utf-8'))
