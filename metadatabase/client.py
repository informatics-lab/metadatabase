"""MongoDB client for storing datasets in MongoDB, and querying and loading from that database."""

from bson.objectid import ObjectId
import iris
import pymongo

from .iris_json import CubeMetaToJSON, CubeToJSON, CubeFromJSON


class _MongoDBClient(object):
    """Handle insertion and querying of a mongoDB database."""
    def __init__(self, host="localhost", port=None):
        """Set up a connection to a MongoDB instance."""
        self.host = host
        self.port = port

        self.client = pymongo.MongoClient(self.host, self.port)

    @property
    def database_names(self):
        return self.client.list_database_names()

    @property
    def collection_names(self):
        db_coll_names = {}
        for name in self.database_names:
            db = self.client.get_database(name)
            db_coll_names[name] = db.list_collection_names()
        return db_coll_names

    def _set_up_collection(self, db_name, collection_name):
        db = self.client[db_name]
        collection = db[collection_name]
        return db, collection

    def _insert_one(self, db_name, collection_name, document):
        _, collection = self._set_up_collection(db_name, collection_name)
        insert_id = collection.insert_one(document).inserted_id
        print(f"Inserted as {insert_id!r} into `{db_name}.{collection_name}`.")

    def insert_one(self, db_name, collection_name, dataset):
        """
        Insert a document (that is, a single Iris cube) into the specified
        database and collection.

        """
        documents = self.adaptor(dataset).cube_dict
        if isinstance(documents, dict):
            self._insert_one(db_name, collection_name, documents)
        else:
            for document in documents:
                self._insert_one(db_name, collection_name, document)

    def insert_many(self, db_name, collection_name, datasets):
        """
        Insert one document per cube in the cubelist into the specified
        database and collection.

        """
        if isinstance(datasets, (iris.cube.Cube, str)):
            datasets = [datasets]

        for dataset in datasets:
            try:
                self.insert_one(db_name, collection_name, dataset)
            except Exception as e:
                itm = dataset.name() if isinstance(dataset, iris.cube.Cube) else dataset
                print(f"Could not insert {itm}. Original exception was:\n{e}")

    def find_one(self, db_name, collection_name, obj_id=None):
        """
        Find a single document in the database, optionally searching by document ID.

        """
        _, collection = self._set_up_collection(db_name, collection_name)

        if obj_id is None:
            result = collection.find_one()
        else:
            result = collection.find_one({"_id": ObjectId(obj_id)})

        return result

    def query_one(self, db_name, collection_name, **query_kw):
        """
        A break from the `pymongo` API, this uses the pymongo `find_one` API to
        run a query that returns the single matching result.

        """
        _, collection = self._set_up_collection(db_name, collection_name)
        return collection.find_one(query_kw)

    def query(self, db_name, collection_name, query_dict):
        """
        A break from the `pymongo` API, this uses the pymongo `find` API to
        run a query that returns all matching results.

        """
        _, collection = self._set_up_collection(db_name, collection_name)
        return collection.find(query_dict)


class CubeJSONClient(_MongoDBClient):
    """
    A client specifically designed for handling entire cubes stored as JSON documents.
    Use the more generic `Client` class for more general interactions.

    """
    def __init__(self, host="localhost", port=27017):
        super().__init__(host, port)

        self.adaptor = CubeToJSON

    def load_and_insert(self, db_name, collection_name, filepath):
        """
        Load the dataset at `filepath` as an Iris cubelist and insert the contents
        of the loaded cubelist into the specified database and collection.

        """
        cubelist = iris.load(filepath)
        self.insert_many(db_name, collection_name, cubelist)

    def _make_loader(self, result, db_name, collection_name,
                     with_optional=True):
        """Encapsulate constructing a `CubeFromJSON` loader object."""
        if with_optional:
            loader = CubeFromJSON(result,
                                  host=self.host, port=self.port,
                                  db_name=db_name, collection_name=collection_name)
        else:
            loader = CubeFromJSON(result)
        return loader

    def find_one(self, db_name, collection_name, obj_id=None):
        """
        Find a single document in the database, optionally searching by document ID.

        """
        result = super().find_one(db_name, collection_name, obj_id)
        loader = self._make_loader(result, db_name, collection_name)
        return loader.load()

    def query_one(self, db_name, collection_name, **query_kw):
        """
        A break from the `pymongo` API, this uses the pymongo `find_one` API to
        run a query that returns the single matching result.

        """
        result = super().query_one(db_name, collection_name, query_kw)
        loader = self._make_loader(result, db_name, collection_name)
        return loader.load()

    def query(self, db_name, collection_name, query_dict):
        """
        A break from the `pymongo` API, this uses the pymongo `find` API to
        run a query that returns all matching results.

        """
        result = super().query(db_name, collection_name, query_dict)
        loader = self._make_loader(result, db_name, collection_name)
        return loader.load()


class Client(_MongoDBClient):
    """
    A client specifically designed for handling entire cubes stored as JSON documents.
    Use the more generic `Client` class for more general interactions.

    """
    def __init__(self, host="localhost", port=27017):
        super().__init__(host, port)

        self.adaptor = CubeMetaToJSON
        self.filename_str = "dataset_ref"

    def find_one(self, db_name, collection_name, obj_id=None):
        """
        Find a single document in the database, optionally searching by document ID.

        """
        return super().find_one(db_name, collection_name, obj_id)[self.filename_str]

    def query_one(self, db_name, collection_name, **query_kw):
        """
        A break from the `pymongo` API, this uses the pymongo `find_one` API to
        run a query that returns the single matching result.

        """
        return super().query_one(db_name, collection_name, query_kw)[self.filename_str]

    def query(self, db_name, collection_name, query_dict):
        """
        A break from the `pymongo` API, this uses the pymongo `find` API to
        run a query that returns all matching results.

        """
        result = super().query(db_name, collection_name, query_dict)
        return [d[self.filename_str] for d in result]