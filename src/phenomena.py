import os
import sys
import logging
from utils import display_log
from pymongo import MongoClient


def phenomena_agent_assets(weather_phenomena, mongo_settings):
    # Connect to mongo and extract all the assets that are subscripted to phenomena
    # MongoBD could use different connection strings according to developer or deployment stage
    client = MongoClient(host=mongo_settings['connection_string'])
    db = client[mongo_settings['database']]
    collection_name: str = mongo_settings['collection']  # type: ignore

    if collection_name is not None:
        assets_collection = db[collection_name]
    else:
        display_log(
            message=f"There is not collection data for {collection_name}")
        sys.exit(__status=0)  # type: ignore

    assets_data = assets_collection.find(
        {"phenomena": {"$in": [weather_phenomena]}})  # type: ignore
    assets_list = [doc for doc in assets_data]
    client.close()

    display_log(
        message="[Phenomena-agent init] mongo DB assets collection consumed")

    return assets_list
