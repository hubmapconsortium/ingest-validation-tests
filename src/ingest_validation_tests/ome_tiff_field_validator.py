import itertools
import re
from functools import partial
from multiprocessing import Pool
from pathlib import Path

import xmlschema
from validator import Validator, check_ome_tiff_file, ome_tiff_globs


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

    def get_schemas(self):
        if self.schemas:
            self._log(f"Prior schemas: {list(self.schemas)}")
            self.schemas = {}
        for schema, regex in self.schema_regex_mapping.items():
            # Iterate through regex for a given schema, if match found add schema to self.schemas and break, check next schema
            for regex_str in regex:
                if re.fullmatch(regex_str, self.assay_type):
                    try:
                        xml_schema = xmlschema.XMLSchema(schema)
                    except xmlschema.XMLSchemaException or SyntaxError:
                        raise Exception(f"Schema {schema} is invalid.")
                    self.schemas[schema] = xml_schema
                    break
        self._log(f"Schemas: {list(self.schemas)}")

    def _collect_errors(self) -> list[str | None]:
        try:
            self.get_schemas()
        except Exception as e:
            return [str(e)]

        filenames_to_test = []
        for glob_expr in ome_tiff_globs:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)
        if not filenames_to_test:
            return []

        pool = Pool(self.threads)
        rslt_list = [
            rslt
            for rslt in pool.imap_unordered(partial(self.errors_by_schema), filenames_to_test)
            if rslt is not None
        ]
        pool.close()
        return self._return_result(
            list(itertools.chain.from_iterable(rslt_list)) if rslt_list else None,
            filenames_to_test,
        )

    def errors_by_schema(self, file: Path) -> list[str] | None:
        try:
            xml_document = check_ome_tiff_file(file)
        except Exception as e:
            return [str(e)]
        compiled_errors = []
        for schema_name, schema in self.schemas.items():
            ome_element_tree = xml_document.get_etree_document()
            errors = {e.reason for e in schema.iter_errors(ome_element_tree) if e.reason}
            if errors:
                msg = f"{file} is not a valid OME.TIFF file per schema '{schema_name.name}': {'; '.join(sorted(errors))}"
                self._log(msg)
                compiled_errors.append(msg)
        return compiled_errors if compiled_errors else None
