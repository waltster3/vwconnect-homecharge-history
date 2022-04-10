import requests
from requests.compat import urljoin
import urllib.parse
import os
import sys
import json
import re
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

# USER SETTINGS
NETWORK_LOCATION = "localhost:8087"   # IP or computer name and port of the io-broker.simple-api

SCHEME = "http"
OBJECTS_PATH = "objects"
GET_PATH = "getPlainValue"
homecharging_records_object = "vw-connect.0.wecharge.homecharging.records.*"

def getObjects():
    query = { "pattern": homecharging_records_object, "prettyPrint": ""}
    url = urllib.parse.urlunparse(
        [SCHEME, NETWORK_LOCATION, OBJECTS_PATH, '', urllib.parse.urlencode(query), '']
    )

    response = requests.get(url)

    if response.ok:
        return response.json()
    else:
        print("Could not retrieve objects via REST API")
        sys.exit(1)

def find_object_names(ids):
    object_names = []

    pattern = "\.([a-zA-Z0-9_]*)$"

    for id in ids:
        match = re.search(pattern, id)

        res = match.group(1)
        if not res in object_names:
            object_names.append(res)
        else:
            # Stop iterating since the new record contains the same object names
            break

    return object_names

def sort_objects_by_type(object_names, ids):
    objects_sorted = {}
    for object_name in object_names:
        ids_for_object = [id for id in ids if id.endswith(object_name)]
        objects_sorted[object_name] = ids_for_object

    return objects_sorted


def make_records(objects_sorted):
    records = {}

    # receiving the first object type (authentication_method) extract the record name
    # and find all matching objects with the same record name
    first_type_of_objects = list(objects_sorted.values())[0]

    for record in first_type_of_objects:
        pattern = ".*records\.([a-zA-Z0-9-:]*)"
        match = re.search(pattern, record)
        record_name = match.group(1)

        #iterate over all object types
        records[record_name] = {}
        for object_name, list_of_objects in objects_sorted.items():
            for obj in list_of_objects:
                if record_name in obj:
                    records[record_name][object_name] = obj
                    break

    return records

def retrieve_objects(record_name, objects):
    values = {}
    print (f"Getting objects for record {record_name}")
    for object_type, object_id in objects.items():
        url = urllib.parse.urlunparse(
            [SCHEME, NETWORK_LOCATION, "/".join([GET_PATH, object_id]), '', '', '']
        )

        response = requests.get(url)

        if response.ok:
            # Some object values are returned in quotes ""
            values[object_type] = response.text.replace('"', '')
        else:
            print("Could not retrieve objects via REST API")
            sys.exit(1)
    return values
    
def get_object_states(records):

    records.pop("latestItem", 0)

    with ThreadPoolExecutor() as executor:
        values = executor.map(retrieve_objects, records.keys(), records.values())

    return values

if __name__ == "__main__":
    objects = getObjects()

    ids = objects.keys()

    object_names = find_object_names(ids)

    objects_sorted = sort_objects_by_type(object_names, ids)

    records = make_records(objects_sorted)

    values = get_object_states(records)

    dataframe = pd.DataFrame(values)

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    dataframe["start_date_time"] = pd.to_datetime(dataframe["start_date_time"], format = date_format, errors="raise" )
    dataframe["stop_date_time"] = pd.to_datetime(dataframe["stop_date_time"], format = date_format, errors="raise" )

    dataframe["year"] = dataframe["start_date_time"].dt.strftime("%Y")
    dataframe["month"] = dataframe["start_date_time"].dt.strftime("%B")
    dataframe["day"] = dataframe["start_date_time"].dt.strftime("%d")

    dataframe.to_csv("export.csv")
