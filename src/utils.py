import os
import sys
import json
import logging
import argparse
from icecream import ic
from pymongo import MongoClient


def path_to_json(path):
    with open(file=path) as text_wrapper:
        deserialized_object: dict[str, str |
                                  dict[str, str]] = json.load(fp=text_wrapper)
        return deserialized_object


def parse_args():
    '''
    Using the 'path_to_json' function to read the contents of the JSON files specified in
    the command-line arguments ('args.kafka_consumer', 'args.kafka_producer', 'args.mongo_connection', 
    'args.phenomena-configuration').
    '''
    parser = argparse.ArgumentParser(
        description="Configuration stage for phenomena-agent")
    parser.add_argument("--kafka-consumer", help="kafka consumer data")
    parser.add_argument("--kafka-producer", help="kafka producer data")
    parser.add_argument("--mongo-connection", help="mongo settings data")
    parser.add_argument("--phenomena-configuration",
                        help="phenomena-agent configurations")
    args = parser.parse_args()
    return args


def display_log(message):
    ic(message)
    logging.info(msg=message)
