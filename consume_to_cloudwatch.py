import kafka_consume as kafka_cons
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os, ssl
import boto3
import json

## KAFKA
consumer_group = "weather_consumer_cw"
consumer_device = "weather_cw"
kafka_topic = "weather"


consumer = kafka_cons.start_consumer(consumer_group,
                                     consumer_device,
                                     kafka_topic,
                                     # NOTE: do not fill CloudWatch with old
                                     # alerts. Just do current
                                     auto_offset_reset='latest',
                                     )

def index_in_cloudwatch(event):
  values = json.loads(event)
  client = boto3.client('cloudwatch')
  response = client.put_metric_data(
      Namespace='weatherIoT',
      MetricData=[
          {
              'MetricName': 'light',
              'Dimensions': [
                  {
                      'Name': 'Device Metrics',
                      'Value': 'nycWeather001'
                  },
              ],
              'Timestamp': values['capture_dttm'],
              'Value': values['light'],
              'Unit': 'None'
          },
          {
              'MetricName': 'barometric_pressure_pa',
              'Dimensions': [
                  {
                      'Name': 'Device Metrics',
                      'Value': 'nycWeather001'
                  },
              ],
              'Timestamp': values['capture_dttm'],
              'Value': values['pressure_pa'],
              'Unit': 'None'
          },
          {
              'MetricName': 'temp_from_pressure_sensor_c',
              'Dimensions': [
                  {
                      'Name': 'Device Metrics',
                      'Value': 'nycWeather001'
                  },
              ],
              'Timestamp': values['capture_dttm'],
              'Value': values['baro_temp_celcius'],
              'Unit': 'None'
          },
          {
              'MetricName': 'humidity',
              'Dimensions': [
                  {
                      'Name': 'Device Metrics',
                      'Value': 'nycWeather001'
                  },
              ],
              'Timestamp': values['capture_dttm'],
              'Value': values['humidity'],
              'Unit': 'None'
          },
          {
              'MetricName': 'temp_celcius',
              'Dimensions': [
                  {
                      'Name': 'Device Metrics',
                      'Value': 'nycWeather001'
                  },
              ],
              'Timestamp': values['capture_dttm'],
              'Value': values['temp_celcius'],
              'Unit': 'None'
          },
      ]
  )
  print(response)
  if not (response['ResponseMetadata']['HTTPStatusCode'] == 200):
      index_in_cloudwatch(event)

print ('Start consuming')
for message in consumer:
 print(message.value.decode('utf-8'))
 index_in_cloudwatch(message.value.decode('utf-8'))
