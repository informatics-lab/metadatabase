"""Test saving a cube into a MongoDB database."""

# Taken from iris.tests.stock (which is simpler than importing due to missing deps.)
from cf_units import Unit
from iris.cube import Cube
import iris.aux_factory
import iris.coords
import iris.coords as icoords
from iris.coords import DimCoord, AuxCoord, CellMethod
from iris.coord_systems import GeogCS, RotatedGeogCS
import numpy as np
import numpy.ma as ma

import mongodb


def lat_lon_cube():
    """
    Returns a cube with a latitude and longitude suitable for testing
    saving to PP/NetCDF etc.
    """
    cube = Cube(np.arange(12, dtype=np.int32).reshape((3, 4)))
    cs = GeogCS(6371229)
    coord = DimCoord(
        points=np.array([-1, 0, 1], dtype=np.int32),
        standard_name="latitude",
        units="degrees",
        coord_system=cs,
    )
    cube.add_dim_coord(coord, 0)
    coord = DimCoord(
        points=np.array([-1, 0, 1, 2], dtype=np.int32),
        standard_name="longitude",
        units="degrees",
        coord_system=cs,
    )
    cube.add_dim_coord(coord, 1)
    return cube


def simple_3d():
    cube = Cube(np.arange(24, dtype=np.int32).reshape((2, 3, 4)))
    cube.long_name = "thingness"
    cube.units = "1"
    wibble_coord = DimCoord(
        np.array([10.0, 30.0], dtype=np.float32), long_name="wibble", units="1"
    )
    lon = DimCoord(
        [-180, -90, 0, 90],
        standard_name="longitude",
        units="degrees",
        circular=True,
    )
    lat = DimCoord([90, 0, -90], standard_name="latitude", units="degrees")
    cube.add_dim_coord(wibble_coord, [0])
    cube.add_dim_coord(lat, [1])
    cube.add_dim_coord(lon, [2])
    return cube


def simple_3d_mask():
    cube = simple_3d()
    cube.data = ma.asanyarray(cube.data)
    cube.data = ma.masked_less_equal(cube.data, 8.0)
    return cube


def realistic_3d():
    data = np.arange(7 * 9 * 11).reshape((7, 9, 11))
    lat_pts = np.linspace(-4, 4, 9)
    lon_pts = np.linspace(-5, 5, 11)
    time_pts = np.linspace(394200, 394236, 7)
    forecast_period_pts = np.linspace(0, 36, 7)
    ll_cs = RotatedGeogCS(37.5, 177.5, ellipsoid=GeogCS(6371229.0))

    lat = icoords.DimCoord(
        lat_pts,
        standard_name="grid_latitude",
        units="degrees",
        coord_system=ll_cs,
    )
    lon = icoords.DimCoord(
        lon_pts,
        standard_name="grid_longitude",
        units="degrees",
        coord_system=ll_cs,
    )
    time = icoords.DimCoord(
        time_pts, standard_name="time", units="hours since 1970-01-01 00:00:00"
    )
    forecast_period = icoords.DimCoord(
        forecast_period_pts, standard_name="forecast_period", units="hours"
    )
    height = icoords.DimCoord(1000.0, standard_name="air_pressure", units="Pa")
    cube = iris.cube.Cube(
        data,
        standard_name="air_potential_temperature",
        units="K",
        dim_coords_and_dims=[(time, 0), (lat, 1), (lon, 2)],
        aux_coords_and_dims=[(forecast_period, 0), (height, None)],
        attributes={"source": "Iris test case"},
    )
    return cube


if __name__ == "__main__":
    r3d_cube = realistic_3d()
    db_name = "iris_stock_data"
    collection_name = "datasets"

    mdbc = mongodb.client.MongoDBClient()
    mdbc.insert_one(db_name, collection_name, r3d_cube)
