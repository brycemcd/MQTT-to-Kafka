"""Reads values coming in off the kafka queue and pushes them to Cloudwatch for
as a first effort in graphing"""

import json
import boto3
import kafka_consume as kafka_cons

## KAFKA
CONSUMER_GROUP = "weather_consumer_cw"
CONSUMER_DEVICE = "weather_cw"
KAFKA_TOPIC = "weather"


CONSUMER = kafka_cons.start_consumer(CONSUMER_GROUP,
                                     CONSUMER_DEVICE,
                                     KAFKA_TOPIC,
                                     # NOTE: do not fill CloudWatch with old
                                     # alerts. Just do current
                                     auto_offset_reset="latest",
                                    )

METRIC_NAMES = [
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
    if value < 0:
        return False

    if "mq" in metric_name and value > 1000:
        return False

    return True

def make_metrics(values):
    """Creates a list of metrics to push to Cloudwatch"""

    metrics = []
    for metric in METRIC_NAMES:
        if valid_metric(metric, values[metric]):
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
    """Main event processor. Reads message off of Kafka queue, makes calls to
    check the validity of the values of the message (wrt Cloudwatch), creates
    metrics and pushes them to Cloudwatch"""

    values = json.loads(event)
    client = boto3.client("cloudwatch")

    metrics = make_metrics(values)
    response = client.put_metric_data(
        Namespace="weatherIoT",
        MetricData=metrics
    )
    print("Request: %s" % json.dumps(metrics))
    print("Response: %s" % response)
    # make_metrics(values)
    # print()
    # print("---")
    if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        index_in_cloudwatch(event)

if __name__ == '__main__':
    print("Starting consumer")
    for message in CONSUMER:
        print(message.value.decode("utf-8"))
        index_in_cloudwatch(message.value.decode("utf-8"))
