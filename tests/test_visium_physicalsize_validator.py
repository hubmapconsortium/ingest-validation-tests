from unittest.mock import Mock

from ome_utils import strip_namespace_and_parse
from visium_physicalsize_validator import VisiumPhysicalsizeValidator

from tests.test_tiff_validators_base_class import TestTiffValidators

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
    "SizeX": "1.0",
    "SizeY": "1.0",
    "SizeZ": "1.0",
}
BASE_OME_XML = '<?xml version="1.0" encoding="UTF-8"?><!-- Warning: this comment is an OME-XML metadata block, which contains crucial dimensional parameters and other important metadata. Please edit cautiously (if at all), and back up the original data before doing so. For more information, see the OME-TIFF web site: http://www.openmicroscopy.org/site/support/ome-model/ome-tiff/. --><OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Creator="OME Bio-Formats 5.2.2" UUID="urn:uuid:2bc2aa39-30d2-44ee-8399-c513492dd5de" xsi:schemaLocation="http://www.openmicroscopy.org/Schemas/OME/2016-06 http://www.openmicroscopy.org/Schemas/OME/2016-06/ome.xsd"><Image ID="Image:0" Name="single-channel.ome.tif"><Pixels BigEndian="true" DimensionOrder="XYZCT" ID="Pixels:0" SizeC="1" SizeT="1" SizeX="439" SizeY="167" SizeZ="1" Type="int8" PhysicalSizeX="10.0" PhysicalSizeY="10.0" PhysicalSizeZ="50.0"><Channel ID="Channel:0:0" SamplesPerPixel="1"><LightPath/></Channel><TiffData FirstC="0" FirstT="0" FirstZ="0" IFD="0" PlaneCount="1"><UUID FileName="single-channel.ome.tif">urn:uuid:2bc2aa39-30d2-44ee-8399-c513492dd5de</UUID></TiffData></Pixels></Image></OME>'


def mutate_ome_xml(pixel_updates: dict[str, str | None] = {}, image: str | None = None):
    combined_pixels = {**DEFAULT_PIXELS, **pixel_updates}
    pixels = " ".join(f'{k}="{v}"' for k, v in combined_pixels.items() if v is not None)
    if image is None:
        image = DEFAULT_IMAGE.format(pixels=pixels)
    return MINIMAL_OME_TEMPLATE.format(body=image)


class TestVisiumPhysicalsizeValidator(TestTiffValidators):

    def test_physicalsize_max_good(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml())
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None

    def test_physicalsize_max_good_no_max(self, monkeypatch):
        validator = VisiumPhysicalsizeValidator("", "visium")
        monkeypatch.setattr(VisiumPhysicalsizeValidator, "maximums", {})
        mock = Mock()
        monkeypatch.setattr(VisiumPhysicalsizeValidator, "check_coordinate", mock)
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml())
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None
        mock.assert_not_called()

    def test_physicalsize_max_good_unit_conversion(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeXUnit": "nm"})
        )
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None

    def test_physicalsize_max_bad_gt_based_on_physicalsize(self, monkeypatch):
        validator = VisiumPhysicalsizeValidator("", "visium")
        monkeypatch.setattr(VisiumPhysicalsizeValidator, "maximums", {"X": 0.1, "Y": 0.1})
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml())
        errors = validator.check_physicalsize_fields("", extracted_ome_xml)
        assert "PhysicalSizeX 10.0 µm * SizeX 1.0 is greater than maximum value 0.1 µm" in errors
        assert "PhysicalSizeY 10.0 µm * SizeY 1.0 is greater than maximum value 0.1 µm" in errors

    def test_physicalsize_max_bad_gt_based_on_size(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"SizeX": "1000", "SizeY": "2000"})
        )
        errors = validator.check_physicalsize_fields("", extracted_ome_xml)
        assert (
            "PhysicalSizeX 10.0 µm * SizeX 1000.0 is greater than maximum value 50.0 µm" in errors
        )
        assert (
            "PhysicalSizeY 10.0 µm * SizeY 2000.0 is greater than maximum value 50.0 µm" in errors
        )

    def test_physicalsize_max_bad_gt_unit_conversion(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeXUnit": "mm"})
        )
        assert (
            "PhysicalSizeX 10.0 mm * SizeX 1.0 is greater than maximum value 50.0 µm"
            in validator.check_physicalsize_fields("", extracted_ome_xml)
        )

    def test_physicalsize_max_bad_value(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeX": "a"})
        )
        assert "PhysicalSizeX 'a' type is str" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_bad_no_physicalsize_value(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeX": None})
        )
        assert "PhysicalSizeX missing" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_bad_no_size_value(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"SizeY": None})
        )
        assert "SizeY missing" in validator.check_physicalsize_fields("", extracted_ome_xml)

    def test_physicalsize_max_bad_units(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(pixel_updates={"PhysicalSizeXUnit": "Pixels"})
        )
        assert "Error in unit parsing or conversion" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_good_multiple_images(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(
            mutate_ome_xml(
                image='<Image><Pixels PhysicalSizeX="0.1" PhysicalSizeY="0.1" SizeX="1" SizeY="1"></Pixels></Image><Image><Pixels PhysicalSizeX="20.0" PhysicalSizeY="20.0" SizeX="1" SizeY="1"></Pixels></Image>'
            )
        )
        assert validator.check_physicalsize_fields("", extracted_ome_xml) is None

    def test_physicalsize_max_bad_no_images(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml(image=""))
        assert "No Image/Pixels found" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )

    def test_physicalsize_max_bad_no_pixels(self):
        validator = VisiumPhysicalsizeValidator("", "visium")
        extracted_ome_xml = strip_namespace_and_parse(mutate_ome_xml(image="<Image></Image>"))
        assert "No Image/Pixels found" in validator.check_physicalsize_fields(
            "", extracted_ome_xml
        )
