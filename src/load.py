import os
import json


def kafka_load_events(producer, producer_topic, values, schema):
    for value in values:
        producer.produce(producer_topic, value=json.dumps(
            obj={"schema": schema, "payload": value}))
        producer.poll(0)
