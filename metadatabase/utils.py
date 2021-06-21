from hashlib import md5


# Similar https://github.com/informatics-lab/tiledb_netcdf/blob/master/nctotdb/data_model.py#L258-L286.
def metadata_hash(cube):
    """
    Produce a predictable but unique hash of the metadata of `cube`.

    The predictable metadata hash of the array is of the form:
        ```name(s)_hash```
    where:
        * `name` is the name(s) of the cube
        * `hash` is an md5 hash of a standardised subset of the data model's metadata.

    The metadata that makes up the hash is as follows:
        * name of cube
        * shape of cube
        * dimension coordinate names
        * grid_mapping name
        * string of cell methods applied to dataset.

    """
    name = cube.name()
    dims = ",".join([c.name() for c in cube.coords(dim_coords=True)])
    grid_mapping = str(cube.coord_system())
    cell_methods = str(cube.cell_methods)

    to_hash = f"{name}_{dims}_{cube.shape}_{grid_mapping}_{cube.cell_methods}"
    metadata_hash = md5(to_hash.encode("utf-8")).hexdigest()

    return f"{name}_{metadata_hash}"
