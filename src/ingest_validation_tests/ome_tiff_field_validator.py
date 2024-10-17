import json
import re
from pathlib import Path
from multiprocessing import Pool
from os import cpu_count
from typing import List, Optional
from functools import partial
import tifffile
import xmlschema
from jsonschema import validate
from ingest_validation_tools.plugin_validator import Validator


def _log(message: str):
    print(message)


def expand_terms(dct: dict, prefix: str = "") -> dict:
    """
    Convert a dict of of XML info as provided by xmlschema to the
    form used in the dictionary of expected fields
    """
    rslt = {}
    expanded_prefix = prefix + "_" if prefix else ""
    for key, val in dct.items():
        if key.startswith("@"):  # terminal element
            rslt[expanded_prefix + key[1:]] = val
        elif key == "$" and isinstance(val, str):  # special case?
            rslt[expanded_prefix + key] = val
        else:
            child_dct = {}
            if isinstance(val, list):
                assert len(val) == 1, "Expected only one element in list of dicts"
                child_dct.update(expand_terms(val[0], expanded_prefix + key))
            elif isinstance(val, dict):
                child_dct.update(expand_terms(val, expanded_prefix + key))
            elif val is None:
                child_dct[expanded_prefix + key] = None
            else:
                raise ValueError(f"list or dict expected; got {type(val)} {val}")
            for key, val in child_dct.items():
                rslt[key] = val
    return rslt


def check_one_prop(key: str, all_prop_dct: dict, this_test: dict) -> None:
    test_type = this_test["dtype"]
    if test_type == "trap":
        # This test is useful when you want to scan lots of ome-tiff files for an
        # example of a new field type
        if key in all_prop_dct:
            raise RuntimeError(f"TRAP: {key} -> {all_prop_dct[key]} vs {this_test}")
        else:
            pass
    elif test_type == "categorical":
        allowed_vals = this_test["allowed_values"]
        assert key in all_prop_dct, f"{key} is required but missing"
        assert all_prop_dct[key] in allowed_vals, (
            f"{key} = {all_prop_dct[key]}" f" not one of {allowed_vals}"
        )
    elif test_type == "integer":
        assert key in all_prop_dct, f"{key} is required but missing"
        assert isinstance(all_prop_dct[key], int), f"{key} = {all_prop_dct[key]}" f" is not an int"
    elif test_type == "float":
        assert key in all_prop_dct, f"{key} is required but missing"
        assert isinstance(all_prop_dct[key], float), (
            f"{key} = {all_prop_dct[key]}" f" is not a float"
        )
    else:
        raise NotImplementedError(f"Unimplemented dtype {test_type} for ome-tiff field")


def _check_ome_tiff_file(file: str, /, tests: dict) -> Optional[str]:
    try:
        with tifffile.TiffFile(file) as tf:
            xml_document = xmlschema.XmlDocument(tf.ome_metadata)
        image_props = xmlschema.to_dict(xml_document)["Image"]
        expanded_props = {}
        for term_dct in image_props:
            expanded_props.update(expand_terms(term_dct))
        error_l = []
        for key in tests:
            try:
                check_one_prop(key, expanded_props, tests[key])
            except AssertionError as excp:
                error_l.append(str(excp))
        if error_l:
            return f"{file} is not a valid OME.TIFF file: {'; '.join(error_l)}"
    except Exception as excp:
        return f"{file} is not a valid OME.TIFF file: {excp}"


class OmeTiffFieldValidator(Validator):
    description = "Recursively test all ome-tiff files for an assay-specific list of fields"
    cost = 1.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        cfg_path = Path(__file__).parent / "ome_tiff_fields.json"
        cfg_list = json.loads(cfg_path.read_text())
        cfg_schema_path = Path(__file__).parent / "ome_tiff_fields_schema.json"
        schema = json.loads(cfg_schema_path.read_text())
        try:
            validate(cfg_list, schema)
        except Exception:
            raise RuntimeError(
                f"Configuration error: {cfg_path}" f" does not satisfy schema {cfg_schema_path}"
            )
        all_tests = {}
        for test_set in cfg_list:
            if re.fullmatch(test_set["re"], self.assay_type):
                all_tests.update(test_set["fields"])

        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        pool = Pool(threads)
        _log(f"Threading at OmeTiffFieldValidator with {threads}")
        filenames_to_test = []
        for glob_expr in [
            "**/*.ome.tif",
            "**/*.ome.tiff",
            "**/*.OME.TIFF",
            "**/*.OME.TIF",
        ]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)

        rslt_list: List[Optional[str]] = list(
            rslt
            for rslt in pool.imap_unordered(
                partial(_check_ome_tiff_file, tests=all_tests), filenames_to_test
            )
            if rslt is not None
        )
        if rslt_list:
            return rslt_list
        elif filenames_to_test:
            return [None]
        else:
            return []
