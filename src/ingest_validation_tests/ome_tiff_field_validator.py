import itertools
import re
from functools import partial
from multiprocessing import Pool
from os import cpu_count
from pathlib import Path
from typing import List, Optional

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator  # type: ignore


def _log(message: str):
    print(message)


class OmeTiffFieldValidator(Validator):
    description = "Recursively test all ome-tiff files for an assay-specific list of fields"
    cost = 1.0
    version = "1.0"
    schemas = {}
    """
    To add a new schema, first create a derivative XSD schema based on the OME XML schema
    (ome.xsd at https://www.openmicroscopy.org/Schemas/) and add to `ome_tiff_schemas` dir.
    Then add to schema_regex_mapping as path_to_schema: [regex_strings_for_relevant_assay_type(s)].

    Note: Schemas should only make the default ome.xsd more restrictive (optional -> required,
    limiting valid categorical values, making min/max more conservative, etc) so as not to
    conflict with the base OME XML spec. `xmlschema.XmlDocument` will validate against base OME schema.

    Files in an upload are validated against all schemas where the assay name matches the regex,
    so make sure your schema does not conflict meaningfully with other relevant schemas (or
    consider refactoring to only validate against a single schema).
    """
    schema_regex_mapping = {
        # Required PhysicalSizeX/Y
        Path(__file__).parent
        / "ome_tiff_schemas/ome_tiff_field_schema_default.xsd": [".*"],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_schemas()

    def get_schemas(self):
        if self.schemas:
            _log(f"Prior schemas: {list(self.schemas)}")
            self.schemas = {}
        for schema, regex in self.schema_regex_mapping.items():
            # Iterate through regex for a given schema, if match found add schema to self.schemas and break, check next schema
            for regex_str in regex:
                if re.fullmatch(regex_str, self.assay_type):
                    try:
                        xml_schema = xmlschema.XMLSchema(schema)
                    except xmlschema.XMLSchemaException or SyntaxError:
                        raise RuntimeError(f"Schema {schema} is invalid.")
                    self.schemas[schema] = xml_schema
                    break
        _log(f"Schemas: {list(self.schemas)}")

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        pool = Pool(threads)
        _log(f"Threading at OmeTiffFieldValidator with {threads}")
        filenames_to_test = []
        for glob_expr in [
            "**/*.[oO][mM][eE].[tT][iI][fF]",
            "**/*.[oO][mM][eE].[tT][iI][fF][fF]",
        ]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)
        if not filenames_to_test:
            return []

        rslt_list = [
            rslt
            for rslt in pool.imap_unordered(partial(self.errors_by_schema), filenames_to_test)
            if rslt is not None
        ]
        if rslt_list:
            return list(itertools.chain.from_iterable(rslt_list))
        elif filenames_to_test:
            return [None]
        else:
            return []

    def errors_by_schema(self, file: Path) -> Optional[list[str]]:
        compiled_errors = []
        for schema_name, schema in self.schemas.items():
            with tifffile.TiffFile(file) as tf:
                try:
                    xml_document = xmlschema.XmlDocument(tf.ome_metadata)  # type: ignore | checks below should be sufficient if bad type returned
                except Exception:
                    return [f"{file} is not a valid OME.TIFF file: Failed to read OME XML"]
            ome_element_tree = xml_document.get_etree_document()
            errors = {e.reason for e in schema.iter_errors(ome_element_tree) if e.reason}
            if errors:
                msg = f"{file} is not a valid OME.TIFF file per schema '{schema_name.name}': {'; '.join(sorted(errors))}"
                _log(msg)
                compiled_errors.append(msg)
        if compiled_errors:
            return compiled_errors
