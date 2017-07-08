import kafka_consume as kafka_cons
from elasticsearch import Elasticsearch

## KAFKA
consumer_group = "doorjamb_consumer_es"
consumer_device = "doorjamb_consumer001"
kafka_topic = "doorjamb"

consumer = kafka_cons.start_consumer(consumer_group,
                                     consumer_device,
                                     kafka_topic)

## ES
es_hosts = [
	'http://es01.thedevranch.net',
	'http://es02.thedevranch.net',
	'http://es03.thedevranch.net',
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
