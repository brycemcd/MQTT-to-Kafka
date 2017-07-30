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
                                     auto_offset_reset="latest",
                                     )

metric_names = [
    "baro_temp_celcius",
    "humidity",
    "light",
    "pressure_pa",
    "temp_celcius",
]

def make_metrics(values):
  metrics = []
  for metric in metric_names:
      # print("metric: %s %s %s" % (values[metric], (values[metric] < 0), metric))
      # NOTE values less that 0 are error values of various sorts
      if (values[metric] > 0):
          metrics.append({
                  "MetricName": metric,
                  "Dimensions": [
                      {
                          "Name": "Device Metrics",
                          "Value": "nycWeather001"
                      },
                  ],
                  "Timestamp": values["capture_dttm"],
                  "Value": values[metric],
                  "Unit": "None"
              })
  # print(metrics)
  return metrics

def index_in_cloudwatch(event):
  values = json.loads(event)
  client = boto3.client("cloudwatch")

  metrics = make_metrics(values)
  response = client.put_metric_data(
      Namespace = "weatherIoT",
      MetricData = metrics
  )
  print("Request: %s" % json.dumps(metrics))
  print("Response: %s" % response)
  # make_metrics(values)
  # print()
  # print("---")
  if not (response["ResponseMetadata"]["HTTPStatusCode"] == 200):
      index_in_cloudwatch(event)

print ("Starting consumer")
for message in consumer:
 print(message.value.decode("utf-8"))
 index_in_cloudwatch(message.value.decode("utf-8"))
