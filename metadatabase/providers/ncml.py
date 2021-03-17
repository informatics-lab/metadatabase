import json
import os
import warnings
import xml.etree.ElementTree as ET

from .core import _JSONiser


class NCMLtoJSON(_JSONiser):
    def __init__(self, dataset):
        super().__init__(dataset)

        self._xml = None
        self._xmlns_attr = None
        self._dim_coords = None

        warnings.warn(
            "NcML handling is incomplete. "
            "Your actual conversion mileage may vary, and "
            "you may encounter errors during conversion.")

    @property
    def xml(self):
        if self._xml is None:
            self.handle_dataset()
        return self._xml

    @xml.setter
    def xml(self, value):
        self._xml = value

    def _construct_xml(self):
        cmd = ["ncdump", "-x", self.dataset]
        try:
            result = subprocess.run(cmd, capture_output=True)
            result.check_returncode()
        except:
            raise
        else:
            self.xml = ET.ElementTree(ET.fromstring(res.stdout.decode()))

    def handle_dataset(self):
        _, filetype = os.path.splitext(self.dataset)
        if filetype == ".xml":
            self.xml = ET.parse(self.dataset)
        elif filetype == ".nc":
            self._construct_xml()
        else:
            emsg = f"Dataset reference must be one of XML, NC file; got {filetype[1:]}."
            raise OSError(emsg)

    def construct_tag(self, tagname):
        if self._xmlns_attr is not None:
            full_tag = f"{self._xmlns_attr}{tagname}"
        else:
            full_tag = tagname

        return full_tag

    def _handle_units(self, unit):
        return {"unit": unit["value"],
                "calendar": None}  # NCML doesn't contain calendar metadata.

    def _handle_basic_attrs(self, d, obj):
        # We can get the var_name from the name attribute of the data var itself.
        d["var_name"] = obj.attrib["name"]
        # Pre-populate expected keys with None values and overwrite from children.
        d["standard_name"] = None
        d["long_name"] = None
        d["units"] = None

        for child in obj:
            if child.attrib["name"] == "standard_name":
                d["standard_name"] = child.attrib["value"]
            elif child.attrib["name"] == "long_name":
                d["long_name"] = child.attrib["value"]
            elif child.attrib["name"] == "units":
                unit_dict = self._handle_units(child.attrib)
                d["units"] = unit_dict

        return d

    def _basic_attrs(self, d, objs):
        if len(objs) == 1:
            d = self._handle_basic_attrs(d, objs[0])
        else:
            result = {}
            for obj in objs:
                var_name = obj.attrib["name"]
                var_basic_attrs = self._handle_basic_attrs({}, obj)
                result[var_name] = var_basic_attrs
            d["datasets"] = result
        return d

    def _get_coord_dims(self, coord, dim_coords=False):
        try:
            covered_dims = coord.attrib["shape"].split(" ")
        except KeyError:
            coord_dims = "scalar"
        else:
            if dim_coords:
                # Only ever 1D.
                covered_dims, = covered_dims
                coord_dims = self._dim_coords.index(covered_dims)
            else:
                coord_dims = [self._dim_coords.index(d) for d in covered_dims]
        return coord_dims

    def coord_to_dict(self, var, dim_coords=False):
        coord_dict = self._basic_attrs({}, var)

        coord_dims = self._get_coord_dims(var, dim_coords=dim_coords)
        coord_dict["dims"] = coord_dims

        return coord_dict

    def populate_dict(self):
        xml_root = self.xml.getroot()

        # Handle strange behaviour rendering all tags as '{http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2}netcdf'
        if self._xmlns_attr is None:
            try:
                self._xmlns_attr = f"{xml_root.tag.split('}')[0]}}}"
            except ValueError:
                self._xmlns_attr = ""

        # Find dimensions.
        dims_tagname = self.construct_tag("dimension")
        dims_childs = xml_root.findall(dims_tagname)
        self._dim_coords = [child.attrib["name"] for child in dims_childs]
        print(f"dimension names: {self._dim_coords}")

        # Find variables.
        vars_tagname = self.construct_tag("variable")
        vars_childs = xml_root.findall(vars_tagname)

        # The data variable is the variable not in the list of dimensions.
        data_vars = []
        for var in vars_childs:
            if var.attrib["name"] not in dimension_names:
                data_vars.append(var)
        data_var_names = [dv.attrib["name"] for dv in data_vars]
        print(data_var_names)

        # Find dimension coordinates from the `shape` attribute of the data variable.
        covered_dims = []
        for data_var in data_vars:
            print(data_var)
            print(data_var.attrib["name"])
            dim_names = data_var.attrib["shape"].split(" ")
            covered_dims.extend(dim_names)
        self._dim_coords = list(set(covered_dims))
        other_coord_names = list(set(dimension_names) - set(self._dim_coords) - set(data_var_names))

        # Handle core metadata.
        base_dict = {"mime_type": "nc",
                     "dataset_ref": xml_root.attrib["location"]}
        self.content_dict = self._basic_attrs(base_dict, data_vars)

        # Handle all coordinates.
        dim_coords_dict = {}
        aux_coords_dict = {}
        for var in vars_childs:
            child_var_name = var.attrib["name"]
            if child_var_name in dim_coord_names:
                dim_coord_dict = self.coord_to_dict(var, dim_coords=True)
                dim_coords_dict[child_var_name] = dim_coord_dict
            elif child_var_name in other_coord_names:
                aux_coord_dict = self.coord_to_dict(var, dim_coords=False)
                aux_coords_dict[child_var_name] = aux_coord_dict
            else:
                # Don't know what this is, but it's probably the data var, which is handled elsewhere.
                pass

        # Add dim coordinates.
        self.content_dict["dim_coords"] = dim_coords_dict

        # Add other coordinates if there are any.
        if len(other_coord_names):
            self.content_dict["aux_coords"] = aux_coords_dict

        # Handle attributes.
        attrs_tagname = self.construct_tag("attribute")
        attrs_childs = xml_root.findall(attrs_tagname)

        attrs_dict = {}
        for attr in attrs_childs:
            name = attr.attrib["name"]
            value = attr.attrib["value"]
            attrs_dict[name] = value
        self.content_dict["attributes"] = attrs_dict
