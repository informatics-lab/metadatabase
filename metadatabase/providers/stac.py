"""Convert Iris cubes to STAC items, and groups of files to STAC Catalogs."""

import json
import os

import iris
import jsonschema
import numpy as np

from .core import _JSONiser
from ..utils import metadata_hash


class CubetoSTACItem(_JSONiser):
    """
    Convert the contents of an Iris cube to a JSON file that's a
    valid STAC Item.

    """

    defaults = {
        "stac_version": "1.0.0-rc.1",
        "stac_extensions": [
            "https://stac-extensions.github.io/datacube/v1.0.0/schema.json"
        ],
        "type": "Feature",
    }

    def __init__(self, dataset, validate=True):
        super().__init__(dataset)

        self.validate = validate
        self._cubes = None
        self._jsons = []

        self._schema_defn = "https://stac-extensions.github.io/datacube/v1.0.0/schema.json"

    @property
    def cubes(self):
        if self._cubes is None:
            self.cubes = iris.load(self.dataset)
        return self._cubes

    @cubes.setter
    def cubes(self, value):
        self._cubes = value

    def coord_to_dict(self, coord, axis=None, dim_coords=False):
        coord_dict = {}

        if axis is not None:
            coord_dict["axis"] = axis

        if axis in ["x", "y"]:
            coord_dict["type"] = "spatial"
            coord_dict["reference_system"] = coord.coord_system.as_cartopy_crs().proj4_params["proj"]
        elif axis == "z":
            coord_dict["type"] = "spatial"
        elif axis == "t":
            coord_dict["type"] = "temporal"
        else:
            coord_dict["type"] = "other"  # XXX may be a better value for this.

        npoints = np.prod(*coord.shape)
        if npoints <= 10:
            coord_dict["values"] = coord.points.tolist()
        else:
            coord_dict["extent"] = [coord.points.reshape(-1)[0],
                                    coord.points.reshape(-1)[-1]]

        coord_dict["unit"] = str(coord.units)

        return coord_dict

    def _make_title(self, cube):
        return ln if (ln := cube.long_name) is not None else cube.name()

    def _cube_geom(self, cube):
        """Figure out the horizontal extent of the cube."""
        x_coord, = cube.coords(axis="x", dim_coords=True)
        y_coord, = cube.coords(axis="y", dim_coords=True)
        x_min, x_max = x_coord[0], x_coord[-1]
        y_min, y_max = y_coord[0], y_coord[-1]
        return x_min, x_max, y_min, y_max

    def _handle_properties(self, cube):
        pd = {}

        title = self._make_title(cube)
        pd["title"] = title

        # Handle STAC schema key `properties.dimensions`.
        dims = ["t", "z", "y", "x"]
        coords_dict = {}
        for dim in dims:
            try:
                coord, = cube.coords(axis=dim, dim_coords=True)
            except ValueError:
                pass
            else:
                name = coord.name()
                contents = self.coord_to_dict(coord, axis=dim, dim_coords=True)
                coords_dict[name] = contents

        # Add aux coords.
        for coord in cube.coords(dim_coords=False):
            name = coord.name()
            contents = self.coord_to_dict(coord, dim_coords=False)
            coords_dict[name] = contents

        pd["cube:dimensions"] = coords_dict
        return pd

    def _handle_assets(self, cube):
        ad = {}

        _, filetype = os.path.splitext(self.dataset.lower())
        if filetype == ".nc":
            app_type = "netcdf"
        elif filetype == ".pp":
            app_type = "PP"
        elif filetype in [".grib", ".grib2"]:
            app_type = "GRIB"
        else:
            app_type = "other"

        ad["data"] = {"href": self.dataset,
                      "type": f"application/{app_type}",
                      "title": self._make_title(cube)}
        # ad["thumbnail"] = None

        return ad

    def _populate_dict(self, cube):
        d = {**self.defaults}

        # Handle STAC schema key `id`.
        d["id"] = metadata_hash(cube)

        # Handle STAC schema keys `geometry` and `bbox`.
        x_min, x_max, y_min, y_max = self._cube_geom(cube)
        d["bbox"] = [x_min, y_min, x_max, y_max]

        # Handle STAC schema key `properties`, including `properties.dimensions`.
        properties_dict = self._handle_properties(cube)
        d["properties"] = properties_dict

        # Handle STAC schema key `assets`.
        assets_dict = self._handle_assets(cube)
        d["assets"] = assets_dict

        # Handle STAC schema key `links`.
        self_link_dict = {"rel": "self",
                          "href": self.dataset}
        d["links"] = [self_link_dict]

        # Possibly validate the constructed STAC Item dict.
        if self.validate:
            self._validate_contents(d)

        return d

    def populate_dict(self):
        for cube in self.cubes:
            stac_json = self._populate_dict(cube)
            self._jsons.append(stac_json)

    def _handle_filename(self, filename, title):
        with_index = len(self._jsons) > 1
        name, ext = os.path.splitext(filename)
        if not len(ext):
            ext = ".json"
        if with_index:
            name = f"{name}_{title}"
        return f"{name}{ext}"

    def save(self, filename):
        for json_str in self._jsons:
            title = json_str["properties"]["title"]
            item_filename = self._handle_filename(filename, title)
            super().save(item_filename, itm=json_str)

    def dump_string(self):
        return (super().dump_string(itm=json_str) for json_str in self._jsons)

    def add_to_catalog(self, catalog_ref):
        pass

    def _validate_contents(self, d):
        jsonschema.validate(instance=d,
                            schema=self._schema_defn)


class STACCollection(object):
    pass


class STACCatalog(object):
    """Generate a STAC Catalog from one or more STAC Items."""

    catalog_name = "catalog.json"

    def __init__(self, catalog_dir):
        self.catalog_dir = catalog_dir

        self._catalog_dict = None

    @property
    def catalog_dict(self):
        return self._catalog_dict

    @catalog_dict.setter
    def catalog_dict(self, value):
        self._catalog_dict = value

    def _find_catalog(self, catalog_name):
        # Search for catalog file in catalog dir.
        full_catalog_dir = os.path.join(self.catalog_dir, catalog_name)
        if not os.path.exists(full_catalog_dir):
            raise OSError(f"Catalog file not found: {full_catalog_dir!r}")

        return full_catalog_dir

    def load_catalog(self, catalog_name=None):
        if catalog_name is None:
            catalog_name = self.catalog_name

        catalog_file = self._find_catalog(catalog_name)
        with (catalog_file, 'r') as ojfh:
            self.catalog_dict = json.load(ojfh)

    def save_catalog(self, catalog_name=None):
        if catalog_name is None:
            catalog_name = self.catalog_name

        catalog_file = os.path.join(self.catalog_dir, catalog_name)
        with open(catalog_file, 'w') as owjfh:
            # XXX Warn if overwriting?
            json.dump(self.catalog_dict, owjfh)

    def catalog_dir(self, in_dir):
        """Catalog the contents of a single directory."""
        files = [os.path.join(in_dir, f) for f in os.listdir(in_dir)]
        return self.catalog_files(files)

    def catalog_files(self, files):
        """
        Catalog the contents of every file specified in the input list `files`.
        Each file may be a data file (e.g. NC, PP, GRIB) or a STAC Item (JSON).
        
        """
        for in_file in files:
            _, filetype = os.path.splitext(in_file)
            if filetype == ".json":
                # Already a STAC item?
                pass
            else:
                # Try making a STAC Item from it.
                itemiser = CubetoSTACItem(in_file)
                itemiser.populate_dict()
                itemiser.save(None)  # XXX What file??
                # XXX what do we need now - the dict or the file?
        # XXX What now?

    def update(self, item):
        """Update an existing catalog with a new STAC Item."""
        pass
