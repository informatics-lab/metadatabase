"""Translate between Iris cubes and JSON objects."""


import json
import os

from cf_units import Unit
import dask.array as da
import iris
import iris.coord_systems
import numpy as np
from numpy.ma import is_masked

from .data_proxy import MongoDBDataProxy


class CubeToJSON(object):
    def __init__(self, cube, include_data=True, include_points=True):
        self.cube = cube
        self.include_data = include_data
        self.include_points = include_points

        if not isinstance(self.cube, iris.cube.Cube):
            raise TypeError(f"Expected a single cube, got {self.cube.__class__.__name__!r} instead.")

        self._cube_dict = None
        self._api_version = iris.__version__

    @property
    def cube_dict(self):
        if self._cube_dict is None:
            self.cube_to_dict()
        return self._cube_dict

    @cube_dict.setter
    def cube_dict(self, value):
        self._cube_dict = value

    def _handle_units(self, unit):
        """Handle unit objects, time unit or otherwise."""
        return {"unit": str(unit),
                "calendar": unit.calendar}

    def _store_data(self, data):
        """Store the cube's data array. XXX make this lazy!"""
        data_dict = {}
        if is_masked(data):
            data_dict["data"] = data.data.tolist()
            data_dict["mask"] = data.mask.tolist()
            data_dict["fill_value"] = str(data.fill_value)
        else:
            data_dict["data"] = data.tolist()
        data_dict["shape"] = self.cube.shape
        data_dict["dtype"] = str(self.cube.dtype)
        return data_dict

    def _basic_attrs(self, d, obj):
        """
        Add basic attributes of `obj` (a cube or a coord), such as names, units and cube attrs,
        to a dictionary `d`.

        """
        # Names.
        d["standard_name"] = obj.standard_name
        d["long_name"] = obj.long_name
        d["var_name"] = obj.var_name

        # Units.
        unit_dict = self._handle_units(obj.units)
        d["units"] = unit_dict

        # Object attributes (metadata).
        d["attributes"] = obj.attributes

        return d

    def _get_coord_dims(self, coord, dim_coords=False):
        coord_dims = self.cube.coord_dims(coord)
        # Explicitly denote scalar coords.
        if not len(coord_dims):
            coord_dims = "scalar"
        elif dim_coords:
            # A DimCoord will only ever be 1D.
            coord_dims, = coord_dims
        return coord_dims

    def _attrs_as_dict(self, attrs):
        return {k: v for (k, v) in attrs}

    def _cs_to_dict(self, coord_system):
        cs_name = coord_system.grid_mapping_name
        cs_dict = self._attrs_as_dict(coord_system._pretty_attrs())
        cs_dict["name"] = cs_name
        # Lat-lon coord systems do not need to define an ellipsoid.
        if cs_name != "latitude_longitude":
            # We don't want the default ellipsoid attr, which is a class instance.
            cs_dict["ellipsoid"] = self._attrs_as_dict(coord_system.ellipsoid._pretty_attrs())
        return cs_dict

    def cell_method_to_dict(self, cm):
        """Convert an Iris cell method to a dictionary."""
        cm_dict = {"method": cm.method,
                   "coords": ','.join(cm.coord_names),
                   "intervals": ','.join(cm.intervals),
                   "comments": ':,:'.join(cm.comments)}
        return cm_dict

    def coord_to_dict(self, coord, coord_dims, dim_coords=False):
        """
        Convert an Iris coord to a dictionary.

        XXX not currently handled:
          * bounds.

        """
        # Store top-level attributes and dimensionality.
        coord_dict = self._basic_attrs({}, coord)
        if not dim_coords and len(coord_dims) == 1:
            coord_dims, = coord_dims
        coord_dict["dims"] = coord_dims

        # Store points metadata, and possibly points too.
        coord_points = coord.points.tolist()
        coord_dict["min"] = coord_points[0]
        coord_dict["max"] = coord_points[-1]
        coord_dict["step"] = None
        coord_dict["npoints"] = len(coord_points)
        coord_dict["shape"] = coord.points.shape
        if self.include_points:
            coord_dict["points"] = coord_points

        # Circular coordinate?
#         coord_dict["circular"] = coord.circular

        # Climatological coordinate?
#         coord_dict["climatology"] = coord.climatological

        # Handle coord system.
        cs = coord.coord_system
        coord_dict["coord_system"] = None if cs is None else self._cs_to_dict(cs)

        return coord_dict

    def cube_to_dict(self):
        """
        Convert `self.cube` to a dictionary, to save as JSON.

        XXX not currently handled:
          * lazy data preservation.
          * masked data.
          * aux factories
          * cell measures
          * ancillary variables.

        """
        # Store top level cube attributes (names, units, global attrs etc.)
        self.cube_dict = self._basic_attrs({}, self.cube)

        # Record the API version.
        self.cube_dict["api_version"] = self._api_version

        if self.include_data:
            # Store data. XXX this will realise data; a better solution would be to stream it.
            data_dict = self._store_data(self.cube.data)
            self.cube_dict["data"] = data_dict

        # Store dimension coordinates.
        dim_coords_dict = {}
        for coord in self.cube.coords(dim_coords=True):
            coord_dim = self._get_coord_dims(coord)
            coord_dict = self.coord_to_dict(coord, coord_dim, dim_coords=True)
            dim_coords_dict[coord.name()] = coord_dict
        self.cube_dict["dim_coords"] = dim_coords_dict

        # Store auxiliary and scalar coordinates.
        aux_coords_dict = {}
        for coord in self.cube.coords(dim_coords=False):
            coord_dims = self._get_coord_dims(coord)
            coord_dict = self.coord_to_dict(coord, coord_dims, dim_coords=False)
            aux_coords_dict[coord.name()] = coord_dict
        self.cube_dict["aux_coords"] = aux_coords_dict

        # Store cell methods, or an empty list if there are none.
        if len(self.cube.cell_methods):
            cms_dict = {}
            for i, cm in enumerate(self.cube.cell_methods):
                cm_dict = self.cell_method_to_dict(cm)
                cms_dict[str(i)] = cm_dict
            self.cube_dict["cell_methods"] = cms_dict
        else:
            self.cube_dict["cell_methods"] = None

        # TODO store aux factories.
        # TODO store cell measures.
        # TODO store ancilliary variables.

    def save(self, filename):
        """Save the dict representation of `self.cube` as a JSON file."""
        with open(filename, 'w') as ojfh:
            json.dump(self.cube_dict, ojfh)

    def dump_string(self):
        """Dump the cube dictionary as a JSON string."""
        return json.dumps(self.cube_dict)


class CubeMetaToJSON(object):
    def __init__(self, dataset_ref):
        self.dataset_ref = dataset_ref

        self._cubes = None
        self._jsonisers = []

    @property
    def cubes(self):
        if self._cubes is None:
            self._load()
        return self._cubes

    @cubes.setter
    def cubes(self, value):
        self._cubes = value

    @property
    def cube_dict(self):
        if len(self._jsonisers) == 0:
            self.cube_to_dict()
        return (jsoniser.cube_dict for jsoniser in self._jsonisers)

    def _load(self, custom_load_fn=None):
        """
        Load the dataset specified by `self.dataset_ref` as one or more Iris cubes.
        
        If `self.dataset_ref` points to a non-POSIX location, you can also specify a custom loader
        to load the data and turn it into Iris cubes. The custom loader function must take one argument
        (`self.dataset_ref`) and return an Iris CubeList.

        """
        if custom_load_fn is not None:
            self.cubes = custom_load_fn(self.dataset_ref)
        else:
            self.cubes = iris.load(self.dataset_ref)

    def cube_to_dict(self):
        for cube in self.cubes:
            jsoniser = CubeToJSON(cube, include_data=False, include_points=False)
            jsoniser.cube_to_dict()
            jsoniser.cube_dict["dataset_ref"] = self.dataset_ref
            jsoniser.cube_dict["mime_type"] = os.path.splitext(self.dataset_ref)[1][1:].lower()
            self._jsonisers.append(jsoniser)

    def _handle_filename(self, filename, idx):
        with_index = len(self._jsonisers) > 1
        name, ext = os.path.splitext(filename)
        if not len(ext):
            ext = ".json"
        if with_index:
            name = f"{name}_{idx}"
        return f"{name}{ext}"

    def save(self, filename):
        for i, jsoniser in self._jsonisers:
            cube_filename = self._handle_filename(filename, i)
            jsoniser.save(cube_filename)

    def dump_string(self):
        return (jsoniser.dump_string() for jsoniser in self._jsonisers)


class CubeFromJSON(object):
    coord_systems_lookup = {'latitude_longitude': iris.coord_systems.GeogCS,
                            'rotated_latitude_longitude': iris.coord_systems.RotatedGeogCS,
                            'mercator': iris.coord_systems.Mercator}

    def __init__(self, documents,
                 host=None, port=None, db_name=None, collection_name=None):
        self.documents = documents
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name

    def _str_to_num(self, num_str):
        try:
            result = int(num_str)
        except ValueError:
            result = float(num_str)
        return result

    def _build_data(self, data_dict):
        """Convert the data-containing dict back into a (possibly masked) NumPy array."""
        data = data_dict["data"]
        shape = data_dict["shape"]
        dtype = np.dtype(data_dict["dtype"])
        try:
            mask = data_dict["mask"]
            fill_value = self._str_to_num(data_dict["fill_value"])
        except KeyError:
            data = np.array(data)
        else:
            data = np.ma.masked_array(data, mask=mask, fill_value=fill_value)
        return data.reshape(shape).astype(dtype)

    def load_data(self, data_dict, obj_id):
        optionals = [self.host, self.port, self.db_name, self.collection_name, obj_id]
        if optionals.count(None) == 0:
            lazy_data = MongoDBDataProxy(data_dict["shape"], np.dtype(data_dict["dtype"]),
                                         self.host, self.port,
                                         self.db_name, self.collection_name,
                                         obj_id)
            data = da.from_array(lazy_data)
        else:
            data = self._build_data(data_dict)
        return data

    def _build_units(self, units_dict):
        if units_dict["calendar"] is None:
            units = Unit(unit=units_dict["unit"])
        else:
            units = Unit(**units_dict)
        return units

    def _build_coord_system(self, coord_system_dict):
        if coord_system_dict is None:
            result = None
        else:
            cs_name = coord_system_dict.pop("name")
            ellipsoid_kwargs = coord_system_dict.pop("ellipsoid", None)
            if ellipsoid_kwargs is not None:
                ellipsoid = iris.coord_systems.GeogCS(**ellipsoid_kwargs)
            else:
                ellipsoid = None

            constructor = self.coord_systems_lookup.get(cs_name)
            if constructor is not None:
                if cs_name == "latitude_longitude":
                    # GeogCS is defined with no ellipsoid.
                    result = constructor(**coord_system_dict)
                else:
                    result = constructor(**coord_system_dict, ellipsoid=ellipsoid)
            else:
                raise ValueError(f"Coord system name {coord_system_name!r} is either not known or supported.")
        return result

    def _load_coord(self, coord, dim_coords=False):
        dims = coord["dims"]
        _CoordDefn = iris.coords.DimCoord if (dim_coords or dims=="scalar") else iris.coords.AuxCoord

        points = np.array(coord["points"]).reshape(coord["shape"])
        units = self._build_units(coord["units"])
        coord_system = self._build_coord_system(coord["coord_system"])

        coord = _CoordDefn(coord["points"],
                           standard_name=coord["standard_name"],
                           long_name=coord["long_name"],
                           var_name=coord["var_name"],
                           units=units,
                           attributes=coord["attributes"],
                           coord_system=coord_system)
        return coord, dims

    def _dim_coords_and_dims(self, dim_coords):
        dcad = []
        for _, coord_dict in dim_coords.items():
            coord, dims = self._load_coord(coord_dict, dim_coords=True)
            dcad.append((coord, dims[0]))
        return dcad

    def _aux_coords_and_dims(self, aux_coords):
        acad = []
        scalar_coords = []
        for _, coord_dict in aux_coords.items():
            coord, dims = self._load_coord(coord_dict, dim_coords=False)
            if dims == "scalar":
                scalar_coords.append(coord)
            else:
                acad.append((coord, dims))
        return acad, scalar_coords

    def _build_attrs(self, attrs_dict):
        """Parse the attributes dict, handling known special cases."""
        parsed_attrs = {}
        for key, value in attrs_dict.items():
            # Handle special cases.
            if key.lower() == "stash":
                # Rebuild a STASH instance from the list that is stored.
                parsed_attrs[key] = STASH(*value)
            else:
                # Everything else is transferred verbatim.
                parsed_attrs[key] = value
        return parsed_attrs

    def _cell_methods(self, cell_methods):
        return CellMethod(cell_method_dict["method"],
                          coords=cell_method_dict["coords"].split(","),
                          intervals=cell_method_dict["intervals"].split(","),
                          comments=cell_method_dict["coords"].split(":,:"))

    def load_cube(self, cube_dict):
        """Load a single cube from a single JSON document presented as a Python dictionary."""
        obj_id = cube_dict.get("_id", None)
        data = self.load_data(cube_dict["data"], obj_id)
        dcad = self._dim_coords_and_dims(cube_dict["dim_coords"])
        acad, scalar_coords = self._aux_coords_and_dims(cube_dict["aux_coords"])
        attributes = self._build_attrs(cube_dict["attributes"])

        cube = iris.cube.Cube(
            data,
            standard_name=cube_dict["standard_name"],
            long_name=cube_dict["long_name"],
            var_name=cube_dict["var_name"],
            units=self._build_units(cube_dict["units"]),
            dim_coords_and_dims=dcad,
            aux_coords_and_dims=acad,
            attributes=attributes)

        for coord in scalar_coords:
            cube.add_aux_coord(coord)

        if cube_dict["cell_methods"] is not None:
            for _, method_dict in cube_dict["cell_methods"].items():
                cell_method = self._build_cell_method(method_dict)
                cube.add_cell_method(cell_method)
        return cube

    def load(self):
        """Load documents queried from mongoDB as Iris cubes."""
        cubes = []
        for document in self.documents:
            cube = self.load_cube(document)
            cubes.append(cube)
        if len(cubes) == 1:
            return cubes[0]
        elif len(cubes) > 1:
            return iris.cube.CubeList(cubes)
        else:
            raise ValueError("No documents provided to load; nothing to do.")

    def fileopen(self):
        """
        Open a JSON file at the path stored in `self.documents`
        and construct a cube from the contents of the file.

        """
        with open(self.documents, 'r') as ojfh:
            cube_dict = json.load(ojfh)
            return self.load_cube(cube_dict)
