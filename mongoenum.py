#!/usr/bin/env python3
import sys
from argparse import ArgumentParser
from pprint import pprint

from pymongo import MongoClient


def enum_collections(client, db_name):
    cols = []
    for col_item in client[db_name].list_collections():
        stats = client[db_name].command("collstats", col_item["name"])
        cols.append(
            {
                "name": col_item["name"],
                "avgObjSize": stats.get("avgObjSize", 0),
                "count": stats["count"],
                "size": stats["size"],
                "storageSize": stats["storageSize"],
                "indexSizes": stats["indexSizes"],
                "totalIndexSize": stats["totalIndexSize"],
            }
        )
    return cols


def enum_databases(client):
    dbs = []
    for db_item in client.list_databases():
        dbs.append(
            {
                "name": db_item["name"],
                "sizeOnDisk": db_item["sizeOnDisk"],
                "collections": enum_collections(client, db_item["name"]),
            }
        )
    return dbs


def format_size(size):
    suffix = "b"
    suffixes = ["KB", "MB", "GB", "TB"]
    while suffixes and size > 1000:
        size = size / 1000
        suffix = suffixes.pop(0)
    size_str = str(round(size, 1))
    return "{} {}".format(size_str, suffix)


def format_count(size):
    suffix = ""
    suffixes = ["K", "M", "B"]
    while suffixes and size > 1000:
        size = size / 1000
        suffix = suffixes.pop(0)
    size_str = str(round(size, 1))
    return "{}{}".format(size_str, suffix).strip()


def render_enum_data(data):
    for db_item in sorted(data, key=lambda x: x["sizeOnDisk"], reverse=True):
        print(
            "Database: {} -- {}".format(
                db_item["name"],
                format_size(db_item["sizeOnDisk"]),
            )
        )
        print("Collections:")
        for col in sorted(
            db_item["collections"], key=lambda x: x["size"], reverse=True
        ):
            print(
                "  * {} -- items: {} -- object: {} -- data: {} -- storage: {} -- index: {}".format(
                    col["name"],
                    format_count(col["count"]),
                    format_count(col["avgObjSize"]),
                    format_size(col["size"]),
                    format_size(col["storageSize"]),
                    format_size(col["totalIndexSize"]),
                )
            )
            for name, size in sorted(
                col["indexSizes"].items(), key=lambda x: x[1], reverse=True
            ):
                print("      {:15}: {}".format(name, format_size(size)))
        print("-" * 40)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-u", "--username")
    parser.add_argument("-p", "--password")
    opts = parser.parse_args()
    return opts


def main():
    opts = parse_args()
    client = MongoClient(username=opts.username, password=opts.password)
    data = enum_databases(client)
    render_enum_data(data)


if __name__ == "__main__":
    main()
