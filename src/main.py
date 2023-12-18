
import os
import time
import sys
import json
import logging
import datetime
import argparse
from datetime import datetime
from assets import build_tessellation, upsert_tessellation, drop_tessellation
from events import reanalisis
from load import kafka_load_events
from icecream import ic
from schemas import kafka_producer_schema
from phenomena import phenomena_agent_assets
from utils import path_to_json, display_log, parse_args
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer

LOGFILE = './Phenomena-agent-dev.log'
logging.basicConfig(filename=LOGFILE, filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')
display_log(
    message='[Phenomena-agent init] Hello, Phenomena agent report to the duty')
load_dotenv(dotenv_path='../dev.env')

if __name__ == '__main__':
    # Loading args - configuration stage
    args: argparse.Namespace = parse_args()

    consumer_settings: dict[str, str | dict[str, str]
                            ] = path_to_json(path=args.kafka_consumer)
    producer_settings: dict[str, str | dict[str, str]
                            ] = path_to_json(path=args.kafka_producer)
    phenomena_settings: dict[str, str | dict[str, str]
                             ] = path_to_json(path=args.phenomena_configuration)
    mongo_settings: dict[str, str | dict[str, str]
                         ] = path_to_json(path=args.mongo_connection)
    '''
    Setting configurations for phenomena-agent MongoDB assets collection susbcripted to phenomena
    '''

    assets_list = phenomena_agent_assets(
        weather_phenomena=phenomena_settings['phenomena-agent']['source'],
        mongo_settings=mongo_settings)

    # Build assets tesselation and assets dictionary
    tessellation_results = build_tessellation(
        conf=phenomena_settings['grid-tessellation'], assets_list=assets_list)
    lat_bins = tessellation_results[0]
    lon_bins = tessellation_results[1]
    grid_boxes = tessellation_results[2]
    idx = tessellation_results[3]
    tessellation = tessellation_results[4]
    assets_dict = tessellation_results[5]
    mx = f"[Phenomena-agent init] {len(assets_dict)} assets successfully added to {len(tessellation)} active tessellation boxes"
    display_log(message=mx)

    """
    Attempting to create a Kafka consumer object using the provided consumer settings. It tries to establish a connection 
    to the Kafka bootstrap server specified in the consumer settings If the connection is successful, it logs a message 
    indicating the successful connection. If the connection fails, save an error message in log file and exits the program
    """
    # Build kafka connectors and susbcribe to those topics
    try:
        consumer_weather = Consumer(consumer_settings['authentication'])
        display_log(message="Good Connection to Kafka weather consumer")
    except Exception as e:
        display_log(
            message=f"Bad Connection to Kafka weather consumer - server failed: {e}")
        sys.exit(__status=0)  # type: ignore

    try:
        consumer_weather.subscribe([consumer_settings['topic']])
        display_log(message="Good Subscription to Kafka weather topic")
    except Exception as e:
        display_log(
            message=f"Bad Subscription to Kafka weather topic - server failed: {e}")
        sys.exit(__status=0)  # type: ignore

    try:
        producer_related_weather = Producer(
            producer_settings["authentication"])
        display_log(message="Good Connection to Kafka related weather producer")
    except Exception as e:
        display_log(
            message=f"Bad Connection to Kafka related weather producer - server failed: {e}")
        sys.exit(__status=0)  # type: ignore

    """
    Start real time operation for data streaming. Attemps to receive and apply the algorithms for each weather 
    event and then relate it with the available assets according to the assets meteorological events of its interest
    """
    RELATED_EVENT_SCHEMA = kafka_producer_schema(
        conf=producer_settings['name'],
        source_type=phenomena_settings['phenomena-agent']['source'])  # type: ignore
    display_log(message="[nowcast-pull init] initializing loop...")

    while (True):
        response = consumer_weather.poll(
            consumer_settings["timeout.seconds"])
        if not response:
            continue
        if response.error():
            logging.error("%s", response.error())
            continue
        obj = json.loads(s=response.value())
        if 'payload' in obj:
            weather_data = obj['payload']
            related_weather = reanalisis(weather=weather_data, lon_bins=lon_bins, lat_bins=lat_bins, tessellation=tessellation,
                                            grid_boxes=grid_boxes, idx=idx, conf=phenomena_settings['phenomena-agent'])

            if related_weather:
                ic(related_weather[0])
                kafka_load_events(producer=producer_related_weather, producer_topic=producer_settings['topic'],
                                    values=related_weather, schema=RELATED_EVENT_SCHEMA)
