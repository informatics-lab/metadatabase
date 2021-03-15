"""Translate between Iris cubes and JSON objects."""


import json
import os

from cf_units import Unit
import dask.array as da
import iris
import iris.coord_systems
import numpy as np
from numpy.ma import is_masked

from .core import _JSONiser
from ..data_proxy import MongoDBDataProxy


class CubeToJSON(_JSONiser):
    def __init__(self, dataset, include_data=True, include_points=True):
        super().__init__(dataset)
        
        self.include_data = include_data
        self.include_points = include_points

        if not isinstance(self.dataset, iris.cube.Cube):
            raise TypeError(f"Expected a single cube, got {self.dataset.__class__.__name__!r} instead.")

        self._api_version = iris.__version__

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
        data_dict["shape"] = self.dataset.shape
        data_dict["dtype"] = str(self.dataset.dtype)
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
        coord_dims = self.dataset.coord_dims(coord)
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
            try:
                pretty_attrs = coord_system.ellipsoid._pretty_attrs()
            except AttributeError:
                # Dirty handling of the fact that not all Iris CSs have `_pretty_attrs`.
                cs_dict["ellipsoid"] = None
            else:
                cs_dict["ellipsoid"] = self._attrs_as_dict(pretty_attrs)
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

    def populate_dict(self):
        """
        Convert `self.dataset` to a dictionary, to save as JSON.

        XXX not currently handled:
          * lazy data preservation.
          * masked data.
          * aux factories
          * cell measures
          * ancillary variables.

        """
        # Store top level cube attributes (names, units, global attrs etc.)
        self.content_dict = self._basic_attrs({}, self.dataset)

        # Record the API version.
        self.content_dict["api_version"] = self._api_version

        if self.include_data:
            # Store data. XXX this will realise data; a better solution would be to stream it.
            data_dict = self._store_data(self.dataset.data)
            self.content_dict["data"] = data_dict

        # Store dimension coordinates.
        dim_coords_dict = {}
        for coord in self.dataset.coords(dim_coords=True):
            coord_dim = self._get_coord_dims(coord)
            coord_dict = self.coord_to_dict(coord, coord_dim, dim_coords=True)
            dim_coords_dict[coord.name()] = coord_dict
        self.content_dict["dim_coords"] = dim_coords_dict

        # Store auxiliary and scalar coordinates.
        aux_coords_dict = {}
        for coord in self.dataset.coords(dim_coords=False):
            coord_dims = self._get_coord_dims(coord)
            coord_dict = self.coord_to_dict(coord, coord_dims, dim_coords=False)
            aux_coords_dict[coord.name()] = coord_dict
        self.content_dict["aux_coords"] = aux_coords_dict

        # Store cell methods, or an empty list if there are none.
        if len(self.dataset.cell_methods):
            cms_dict = {}
            for i, cm in enumerate(self.dataset.cell_methods):
                cm_dict = self.cell_method_to_dict(cm)
                cms_dict[str(i)] = cm_dict
            self.content_dict["cell_methods"] = cms_dict
        else:
            self.content_dict["cell_methods"] = None

        # TODO store aux factories.
        # TODO store cell measures.
        # TODO store ancilliary variables.


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
    def content_dict(self):
        if len(self._jsonisers) == 0:
            self.populate_dict()
        return (jsoniser.content_dict for jsoniser in self._jsonisers)

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

    def populate_dict(self):
        for cube in self.cubes:
            jsoniser = CubeToJSON(cube, include_data=False, include_points=False)
            jsoniser.populate_dict()
            jsoniser.content_dict["dataset_ref"] = self.dataset_ref
            jsoniser.content_dict["mime_type"] = os.path.splitext(self.dataset_ref)[1][1:].lower()
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
        for i, jsoniser in enumerate(self._jsonisers):
            cube_filename = self._handle_filename(filename, i)
            jsoniser.save(cube_filename)

    def dump_string(self):
        return (jsoniser.dump_string() for jsoniser in self._jsonisers)


