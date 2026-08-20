import zipfile
from pathlib import Path
from unittest.mock import Mock

import pytest
from ome_tiff_field_validator import OmeTiffFieldValidator
from ome_utils import strip_namespace_and_parse
from test_tiff_validators_base_class import TestTiffValidators
from validator import BASE_OME_XML_SCHEMA

MINIMAL_OME_TEMPLATE = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
    "{body}</OME>"
)
DEFAULT_IMAGE = "<Image><Pixels {pixels}></Pixels></Image>"
DEFAULT_PIXELS = {
    "PhysicalSizeX": "10.0",
    "PhysicalSizeY": "10.0",
    "PhysicalSizeZ": "50.0",
}
BASE_OME_XML = '<?xml version="1.0" encoding="UTF-8"?><!-- Warning: this comment is an OME-XML metadata block, which contains crucial dimensional parameters and other important metadata. Please edit cautiously (if at all), and back up the original data before doing so. For more information, see the OME-TIFF web site: http://www.openmicroscopy.org/site/support/ome-model/ome-tiff/. --><OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Creator="OME Bio-Formats 5.2.2" UUID="urn:uuid:2bc2aa39-30d2-44ee-8399-c513492dd5de" xsi:schemaLocation="http://www.openmicroscopy.org/Schemas/OME/2016-06 http://www.openmicroscopy.org/Schemas/OME/2016-06/ome.xsd"><Image ID="Image:0" Name="single-channel.ome.tif"><Pixels BigEndian="true" DimensionOrder="XYZCT" ID="Pixels:0" SizeC="1" SizeT="1" SizeX="439" SizeY="167" SizeZ="1" Type="int8" PhysicalSizeX="10.0" PhysicalSizeY="10.0" PhysicalSizeZ="50.0"><Channel ID="Channel:0:0" SamplesPerPixel="1"><LightPath/></Channel><TiffData FirstC="0" FirstT="0" FirstZ="0" IFD="0" PlaneCount="1"><UUID FileName="single-channel.ome.tif">urn:uuid:2bc2aa39-30d2-44ee-8399-c513492dd5de</UUID></TiffData></Pixels></Image></OME>'


def mutate_ome_xml(pixel_updates: dict[str, str | None] = {}, image: str | None = None):
    combined_pixels = {**DEFAULT_PIXELS, **pixel_updates}
    pixels = " ".join(f'{k}="{v}"' for k, v in combined_pixels.items() if v is not None)
    if image is None:
        image = DEFAULT_IMAGE.format(pixels=pixels)
    return MINIMAL_OME_TEMPLATE.format(body=image)


class TestOmeTiffFieldValidator(TestTiffValidators):

    def validator(self, test_data_fname, assay_type, tmp_path, coreuse):

        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        return OmeTiffFieldValidator(tmp_path / test_data_path.stem, assay_type, coreuse=coreuse)

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            (
                "test_data/codex_tree_ometiff_bad.zip",
                [
                    ".*/codex_tree_ometiff_bad/tubhiswt_C0_bad.ome.tif is not a valid OME.TIFF file: No XML found in OME.TIFF file.",
                    ".*/codex_tree_ometiff_bad/sample1.ome.tif is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_require_physicalsizexy.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    "codex_tree_ometiff_bad/sample1.ome.tif OME-XML errors: PhysicalSizeX missing; PhysicalSizeY missing",
                    ".*/codex_tree_ometiff_bad/sample2.ome.tif is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_require_physicalsizexy.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    "codex_tree_ometiff_bad/sample2.ome.tif OME-XML errors: PhysicalSizeX missing; PhysicalSizeY missing",
                ],
                "CODEX",
            ),
            ("test_data/codex_tree_ometiff_good.zip", [None], "CODEX"),
            ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_require_physicalsizexy.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    "complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff OME-XML errors: PhysicalSizeX missing; PhysicalSizeY missing",
                ],
                "PAS",
            ),
        ),
    )
    def test_ome_tiff_field_validator(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        validator = self.validator(test_data_fname, assay_type, tmp_path, coreuse=4)
        errors = validator.collect_errors()[:]
        self.check_errors(msg_re_list, errors)
        print(errors)

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            ("test_data/fake_snrnaseq_tree_good.zip", [], "test_dataset_type"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_require_physicalsizexy.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'test_ome_tiff_field_schema.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'; missing required attribute 'PhysicalSizeZ'",
                    "complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff OME-XML errors: PhysicalSizeX missing; PhysicalSizeY missing; PhysicalSizeZ missing",
                ],
                "test_dataset_type",
            ),
            ("test_data/codex_tree_ometiff_good.zip", [None], "CODEX"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_require_physicalsizexy.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    "complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff OME-XML errors: PhysicalSizeX missing; PhysicalSizeY missing",
                ],
                "PAS",
            ),
        ),
    )
    def test_multiple_test_cfgs(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        """
        Make optional fields in default schema required for `test_dataset_type`.
        Make sure PAS fixture from previous test still passes.
        """

        validator = self.validator(test_data_fname, assay_type, tmp_path, coreuse=4)
        validator.schema_regex_mapping[
            Path(__file__).parent.parent / "test_data/test_ome_tiff_field_schema.xsd"
        ] = ["test_dataset_type"]
        if assay_type == "test_dataset_type":
            validator.default_z_max = 10
        validator.get_schemas()
        errors = validator.collect_errors()[:]
        validator.schema_regex_mapping = OmeTiffFieldValidator.schema_regex_mapping
        validator.get_schemas()
        self.check_errors(msg_re_list, errors)

    def test_retrieve_ome_xml_schema(self, tmp_path):
        """
        Make sure that retrieving BASE_OME_XML_SCHEMA works.
        """
        validator = self.validator(
            "test_data/codex_tree_ometiff_good.zip", "test_dataset_type", tmp_path, coreuse=4
        )
        validator.schema_regex_mapping[BASE_OME_XML_SCHEMA] = ["test_dataset_type"]
        validator.get_schemas()
        assert BASE_OME_XML_SCHEMA in validator.schemas
        errors = validator.collect_errors()[:]
        self.check_errors([None], errors)

    ######################
    # PhysicalSize tests #
    ######################

    def test_physicalsize_max_good(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml())
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None

    def test_physicalsize_max_good_no_max(self, monkeypatch):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        monkeypatch.setattr(OmeTiffFieldValidator, "maximums", {})
        mock = Mock()
        monkeypatch.setattr(OmeTiffFieldValidator, "check_coordinate", mock)
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml())
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None
        mock.assert_not_called()

    def test_physicalsize_max_good_unit_conversion(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeXUnit": "nm"})
        )
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None

    def test_physicalsize_max_bad_gt(self, monkeypatch):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        monkeypatch.setattr(OmeTiffFieldValidator, "maximums", {"X": 0.1, "Y": 0.1})
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml())
        errors = validator.check_physicalsize_fields("", extracted_ome_xml)
        assert "PhysicalSizeX 10.0 µm is greater than maximum value 0.1 µm" in errors
        assert "PhysicalSizeY 10.0 µm is greater than maximum value 0.1 µm" in errors

    def test_physicalsize_max_bad_gt_unit_conversion(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeXUnit": "mm"})
        )
        assert (
            "PhysicalSizeX 10.0 mm is greater than maximum value 10.0 µm"
            in validator.check_physicalsize_fields("", extracted_ome_xml)
        )

    def test_physicalsize_max_bad_value(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeX": "a"})
        )
        assert "PhysicalSizeX 'a' type is str" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_bad_no_value(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeX": None})
        )
        assert "PhysicalSizeX missing" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_bad_units(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeXUnit": "Pixels"})
        )
        assert "Error in unit parsing or conversion" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_good_multiple_images(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(
                image='<Image><Pixels PhysicalSizeX="0.1" PhysicalSizeY="0.1"></Pixels></Image><Image><Pixels PhysicalSizeX="20.0" PhysicalSizeY="20.0"></Pixels></Image>'
            )
        )
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None

    def test_physicalsize_max_bad_no_images(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml(image=""))
        assert "No Image/Pixels found" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_bad_no_pixels(self):
        validator = OmeTiffFieldValidator("", "test_dataset_type")
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml(image="<Image></Image>"))
        assert "No Image/Pixels found" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )
