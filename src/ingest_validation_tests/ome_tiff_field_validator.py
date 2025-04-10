import re
from functools import partial
from multiprocessing import Pool
from os import cpu_count
from pathlib import Path
from typing import List, Optional
from xml.etree import ElementTree

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator


def _log(message: str):
    print(message)


def test_func(schemas: list[Path], file: Path):
    for schema in schemas:
        test_schema = xmlschema.XMLSchema(schema)
        with tifffile.TiffFile(file) as tf:
            metadata = tf.ome_metadata
            if not metadata:
                return f"{file} is not a valid OME.TIFF file: Failed to read OME XML"
        ome_element_tree = ElementTree.fromstring(metadata)
        errors = set([e.reason for e in test_schema.iter_errors(ome_element_tree) if e.reason])
        if errors:
            _log(f"Validation failed with schema {schema.name} for {file}: {errors}")
            return f"{file} is not a valid OME.TIFF file per schema {schema.name}: {'; '.join(sorted(errors))}"


class OmeTiffFieldValidator(Validator):
    description = "Recursively test all ome-tiff files for an assay-specific list of fields"
    cost = 1.0
    version = "1.0"
    schemas = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        schema_dir = Path(__file__).parent / "ome_tiff_schemas"
        schema_regex_mapping = {"ome_tiff_field_schema_default.xsd": ".*"}
        for schema, regex in schema_regex_mapping.items():
            schema_path = schema_dir / schema
            try:
                xmlschema.XMLSchema.meta_schema.validate(schema_path)
            except Exception:
                raise RuntimeError(f"Schema {schema} is invalid.")
            if re.fullmatch(regex, self.assay_type):
                self.schemas.append(schema_path)

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
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
            for rslt in pool.imap_unordered(partial(test_func, self.schemas), filenames_to_test)
            if rslt is not None
        )
        if rslt_list:
            return rslt_list
        elif filenames_to_test:
            return [None]
        else:
            return []
