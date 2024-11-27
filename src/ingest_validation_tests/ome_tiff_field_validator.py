import json
import re

from functools import partial
from multiprocessing import Pool
from os import cpu_count
from pathlib import Path
from typing import List, Optional

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator
from jsonschema import validate


def _log(message: str):
    print(message)


def expand_terms(dct: dict, prefix: str = "") -> list:
    """
    Convert a dict of of XML info as provided by xmlschema to the
    form used in the dictionary of expected fields
    """
    rslt = []
    expanded_prefix = prefix + "_" if prefix else ""
    for key, val in dct.items():
        if key.startswith("@"):  # terminal element
            rslt.append((expanded_prefix + key[1:], val))
        elif key == "$" and isinstance(val, str):  # special case?
            rslt.append((expanded_prefix + key, val))
        else:
            child_list_list = []
            if isinstance(val, list):
                for elt in val:
                    child_list_list.append(expand_terms(elt, expanded_prefix + key))
            elif isinstance(val, dict):
                child_list_list.append(expand_terms(val, expanded_prefix + key))
            elif val is None:
                child_list_list.append([(expanded_prefix + key, None)])
            else:
                raise ValueError(f"list or dict expected; got {type(val)} {val}")
            for child_list in child_list_list:
                for key, val in child_list:
                    rslt.append((key, val))
    return rslt


def check_one_prop(key: str, all_prop_list: list, this_test: dict) -> None:
    all_prop_keys = set(key for key, val in all_prop_list)
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
        assert key in all_prop_keys, f"{key} is required but missing"
        for val in [thisval for thiskey, thisval in all_prop_list if thiskey == key]:
            assert val in allowed_vals, (
                f"{key} == {val} is not one of {allowed_vals}"
            )
    elif test_type == "integer":
        assert key in all_prop_keys, f"{key} is required but missing"
        for val in [thisval for thiskey, thisval in all_prop_list if thiskey == key]:
            assert isinstance(val, int), f"{key} = {val} is not an int"
    elif test_type == "float":
        assert key in all_prop_keys, f"{key} is required but missing"
        for val in [thisval for thiskey, thisval in all_prop_list if thiskey == key]:
            assert isinstance(val, float), f"{key} = {val} is not a float"
    else:
        raise NotImplementedError(f"Unimplemented dtype {test_type} for ome-tiff field")


def _check_ome_tiff_file(file: str, /, tests: dict) -> Optional[str]:
    try:
        with tifffile.TiffFile(file) as tf:
            xml_document = xmlschema.XmlDocument(tf.ome_metadata)
    except Exception as excp:
        return f"{file} is not a valid OME.TIFF file: Failed to read OME XML"

    try:
        image_props = xmlschema.to_dict(xml_document)["Image"]
        expanded_props = []
        for term_dct in image_props:
            expanded_props.extend(expand_terms(term_dct))
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

        if not all_tests:
            return []  # nothing to test for this assay

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

        # TODO: turn back on when issues with XML parsing are resolved
        # still collecting files so we know if this plugin *should* have run

        rslt_list = []
        for fname in filenames_to_test:
            rslt = _check_ome_tiff_file(fname, tests=all_tests)
            if rslt is not None:
                _log(f"Result of _check_ome_tiff_file({fname}) is {rslt}")
                rslt_list.append(rslt)
#         rslt_list: List[Optional[str]] = list(
#            for rslt in pool.imap_unordered(
#                partial(_check_ome_tiff_file, tests=all_tests), filenames_to_test
#            )
#            if rslt is not None
#         )
        if rslt_list:
            return rslt_list
        elif filenames_to_test:
            return [None]
        else:
            return []
