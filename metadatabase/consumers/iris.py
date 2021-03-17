import json

from cf_units import Unit
import dask.array as da
import iris
import iris.coord_systems
import numpy as np

from .data_proxy import MongoDBDataProxy


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
