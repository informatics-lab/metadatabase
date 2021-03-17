import json


class _JSONiser(object):
    """Abstract class for JSON-ising (converting to JSON) various objects."""
    def __init__(self, dataset):
        self.dataset = dataset
        self._content_dict = None

    @property
    def content_dict(self):
        if self._content_dict is None:
            self.populate_dict()
        return self._content_dict

    @content_dict.setter
    def content_dict(self, value):
        self._content_dict = value

    def _handle_units(self):
        """
        Convert units metadata to a dictionary. Must be overridden in concrete implementations.

        """
        raise NotImplementedError

    def _basic_attrs(self):
        """
        Convert basic metadata attributes, such as names and units, to a dictionary.
        Must be overridden in concrete implementations.

        """
        raise NotImplementedError

    def coord_to_dict(self):
        """
        Convert coordinate metadata to a dictionary. Must be overridden in concrete implementations.

        """
        raise NotImplementedError

    def populate_dict(self):
        """
        Populate the metadata dictionary with all relevant metadata from the input data object.
        Must be overridden in concrete implementations.

        """
        raise NotImplementedError

    def save(self, filename):
        """Save the dict representation of `self.dataset` as a JSON file."""
        with open(filename, 'w') as ojfh:
            json.dump(self.content_dict, ojfh)

    def dump_string(self):
        """Dump the cube dictionary as a JSON string."""
        return json.dumps(self.content_dict)