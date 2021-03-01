"""Provides a data proxy for deferring access to data from a mongoDB query."""


from bson.objectid import ObjectId
import numpy as np
import pymongo


# Inspired by https://github.com/SciTools/iris/blob/master/lib/iris/fileformats/netcdf.py#L418.
class MongoDBDataProxy:
    """A proxy to the data of a single TileDB array attribute."""

    __slots__ = ("shape", "dtype", "host", "port", "db_name", "collection_name", "obj_id")

    def __init__(self, shape, dtype,
                 host, port, db_name, collection_name, obj_id):
        self.shape = shape
        self.dtype = dtype
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.obj_id = obj_id

    @property
    def ndim(self):
        return len(self.shape)

    def _str_to_num(self, num_str):
        """Convert a number expressed as a string to an int or a float."""
        try:
            result = int(num_str)
        except ValueError:
            result = float(num_str)
        return result

    def _load_data(self, data_dict):
        """Convert the data-containing dict back into a (possibly masked) NumPy array."""
        data = data_dict["data"]
        try:
            mask = data_dict["mask"]
            fill_value = self._str_to_num(data_dict["fill_value"])
        except KeyError:
            data = np.array(data)
        else:
            data = np.ma.masked_array(data, mask=mask, fill_value=fill_value)
        return data.reshape(self.shape).astype(self.dtype)

    def __getitem__(self, keys):
        # Set up a client connection.
        mdb_client = pymongo.MongoClient(self.host, self.port)
        db = mdb_client[self.db_name]
        collection = db[self.collection_name]

        document = collection.find_one({"_id": ObjectId(self.obj_id)})
        data = self._load_data(document)
        return data[keys]

    def __getstate__(self):
        return {attr: getattr(self, attr) for attr in self.__slots__}

    def __setstate__(self, state):
        for key, value in state.items():
            setattr(self, key, value)