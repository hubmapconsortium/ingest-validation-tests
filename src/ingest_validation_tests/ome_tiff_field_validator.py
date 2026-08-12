import itertools
import re
from functools import cached_property, partial
from multiprocessing import Pool
from pathlib import Path
from xml.etree import ElementTree as ET

import xmlschema
from ome_utils import strip_namespace_and_parse
from validator import (
    FileTypes,
    Validator,
    check_ome_xml,
    extract_ome_xml,
    find_all_files,
)


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
        / "ome_tiff_schemas/ome_tiff_field_schema_require_physicalsizexy.xsd": [".*"],
    }
    default_x_max = 10
    default_y_max = 10
    default_z_max = None

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
        for path in self.paths:
            filenames_to_test.extend(find_all_files(path, FileTypes.OME_TIFF))
        if not filenames_to_test:
            return []

        pool = Pool(self.threads)
        rslt_list = [
            rslt
            for rslt in pool.imap_unordered(partial(self.get_ome_xml_errors), filenames_to_test)
            if rslt is not None
        ]
        pool.close()
        pool.join()
        return self._return_result(
            list(itertools.chain.from_iterable(rslt_list)) if rslt_list else None,
            filenames_to_test,
        )

    def get_ome_xml_errors(self, file: Path) -> list[str] | None:
        try:
            extracted_ome_xml = extract_ome_xml(file)
            xml_document = check_ome_xml(extracted_ome_xml, file)
        except Exception as e:
            return [str(e)]
        compiled_errors = []
        if schema_errors := self.errors_by_schema(file, xml_document):
            compiled_errors.extend(schema_errors)
        if physicalsize_errors := self.check_physicalsize_fields(
            file, strip_namespace_and_parse(extracted_ome_xml)
        ):
            compiled_errors.append(physicalsize_errors)
        return compiled_errors if compiled_errors else None

    def errors_by_schema(
        self, file: Path, xml_document: xmlschema.XmlDocument
    ) -> list[str] | None:
        compiled_errors = []
        for schema_name, schema in self.schemas.items():
            ome_element_tree = xml_document.get_etree_document()
            errors = {e.reason for e in schema.iter_errors(ome_element_tree) if e.reason}
            if errors:
                msg = f"{file} is not a valid OME.TIFF file per schema '{schema_name.name}': {'; '.join(sorted(errors))}"
                self._log(msg)
                compiled_errors.append(msg)
        return compiled_errors

    @cached_property
    def maximums(self):
        """
        Property allows for setting assay-specific maximums,
        e.g. using a switch statement with self.assay_type.
        """
        return {
            "X": self.default_x_max,
            "Y": self.default_y_max,
            "Z": self.default_z_max,
        }

    def check_physicalsize_fields(self, file: Path, xml_etree: ET.Element) -> str | None:
        """
        Compare PhysicalSizeX / Y / Z values in OME XML images against
        values in self.maximums. Catch values that exceed maximum as well
        as missing / malformed values.
        """
        errors = []
        if (xml_image_data := xml_etree.find("Image/Pixels")) is None:
            return f"No Image/Pixels found in file {self.rel_filename_str(file)}"
        for coordinate in ["X", "Y", "Z"]:
            if not (max := self.maximums[coordinate]):
                continue
            if not (value := xml_image_data.get(f"PhysicalSize{coordinate}")):
                errors.append(f"PhysicalSize{coordinate} missing")
                continue
            try:
                if float(value) > max:
                    errors.append(
                        f"PhysicalSize{coordinate} {value} is greater than maximum value {max}"
                    )
            except (ValueError, TypeError):
                errors.append(f"PhysicalSize{coordinate} '{value}' type is {type(value).__name__}")
        if errors:
            error_str = (
                f"{self.rel_filename_str(file)} OME-XML errors: {'; '.join(sorted(errors))}"
            )
            return error_str
