"""Insert data from Iris test and sample data repos into a mongoDB Atlas instance."""

import os

import pymongo

from metadatabase.client import Client


def absolute_path_walker(path):
    absolute_paths = []
    walker = os.walk(path)
    for (thisdir, _, files) in walker:
        paths = [os.path.join(thisdir, fn) for fn in files]
        absolute_paths.extend(paths)
    return absolute_paths


def main(pw, data_dir, db_name="example_data", collection_name="iris_sample_data"):
    conn_str = f"mongodb+srv://iris:{pw}@iris-example-data.omfld.mongodb.net/?retryWrites=true&w=majority"
    client = Client(conn_str)

    sample_data_files = absolute_path_walker(data_dir)

    client.insert_many(db_name, collection_name, sample_data_files)


if __name__ == "__main__":
    import sys
    mongodb_pw = str(sys.argv[1])
    data_dir = str(sys.argv[2])
    db_name = str(sys.argv[3])
    collection_name = str(sys.argv[4])

    main(mongodb_pw, data_dir, db_name, collection_name)
