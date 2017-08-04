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
    "mq135",
    "mq5",
    "mq6",
    "mq9",
]

def valid_metric(metric_name, value):
    """Validates that a metric is within reasonable, reportable parameters.
    Returns True if valid (default) and false if any validity constraints are
    violated. A very hacky, first effort, implementation"""

    # by default, all metrics less than 0 are considered invalid values
    if value < 0 :
        return False

    if "mq" in metric_name and value > 1000 :
        return False

    return True

def make_metrics(values):
  metrics = []
  for metric in metric_names:
      if valid_metric(metric, values[metric]) :
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
