import itertools
import re
from functools import cached_property, partial
from multiprocessing import Pool
from pathlib import Path

import xmlschema
from validator import FileTypes, Validator, check_ome_tiff_file, find_all_files


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
    default_z_max = 10

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
            xml_document = check_ome_tiff_file(file)
        except Exception as e:
            return [str(e)]
        compiled_errors = []
        if schema_errors := self.errors_by_schema(file, xml_document):
            compiled_errors.extend(schema_errors)
        if physicalsize_errors := self.check_physicalsize_fields(file, xml_document):
            compiled_errors.extend(physicalsize_errors)
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
        # TODO: for assay-specific maximums, use e.g. switch
        # statement to get correct values comparing against
        # self.assay_type
        return {
            "x_max": self.default_x_max,
            "y_max": self.default_y_max,
            "z_max": self.default_z_max,
        }

    def check_physicalsize_fields(
        self, file: Path, xml_document: xmlschema.XmlDocument
    ) -> list[str] | None:
        errors = []
        images = xml_document.schema.to_dict(xml_document).get("Image")  # type: ignore
        for i, image in enumerate(images):
            xml_image_data = image.get("Pixels")
            physicalsizex = xml_image_data.get("@PhysicalSizeX")
            physicalsizey = xml_image_data.get("@PhysicalSizeY")
            # TODO: not sure about role of Z yet
            physicalsizez = xml_image_data.get("@PhysicalSizeZ")
            for field_name, value, max_value in [
                ("PhysicalSizeX", physicalsizex, self.maximums["x_max"]),
                ("PhysicalSizeY", physicalsizey, self.maximums["y_max"]),
                ("PhysicalSizeZ", physicalsizez, self.maximums["z_max"]),
            ]:
                try:
                    if int(value) > max_value:
                        errors.append(
                            f"In file {self.rel_filename_str(file)}, image {i}, {field_name} with value {value} is greater than maximum value {max_value}"
                        )
                except (ValueError, TypeError):
                    errors.append(
                        f"In file {self.rel_filename_str(file)}, image {i}, {field_name} with value {value} cannot be cast to int (type is {type(value)}"
                    )
