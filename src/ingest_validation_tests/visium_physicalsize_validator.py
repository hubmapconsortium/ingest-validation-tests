from dataclasses import dataclass
from functools import cached_property, partial
from multiprocessing import Pool
from pathlib import Path
from xml.etree import ElementTree as ET

from ome_utils import strip_namespace_and_parse
from pint import DimensionalityError, UndefinedUnitError, UnitRegistry
from validator import FileTypes, Validator, extract_ome_xml, find_all_files


@dataclass
class SizeData:
    coordinate: str
    physicalsize: float
    size: float
    unit: str


class VisiumPhysicalsizeValidator(Validator):
    description = "Test image sizes in Visium OME-TIFF files"
    cost = 2.0
    version = "1.0"
    assay_type = ["visium (with probes)", "visium (no probes)"]

    default_x_max = 50.0
    default_y_max = 50.0
    default_z_max = None

    @cached_property
    def filenames_to_test(self) -> list[Path]:
        filenames_to_test = []
        for path in self.paths:
            filenames_to_test.extend(find_all_files(path, FileTypes.OME_TIFF))
        return filenames_to_test

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

    def _collect_errors(self) -> list[str | None]:
        if not self.filenames_to_test:
            return []

        pool = Pool(self.threads)
        try:
            raw_rslt_list = pool.imap_unordered(
                partial(self.get_physicalsize_errors), self.filenames_to_test
            )
        except Exception as e:
            raw_rslt_list = [str(e)]
        finally:
            pool.close()
            pool.join()

        rslt_list = [rslt for rslt in raw_rslt_list if rslt is not None]
        return self._return_result(rslt_list if rslt_list else None, self.filenames_to_test)

    def get_physicalsize_errors(self, file: Path) -> str | None:
        try:
            extracted_ome_xml = extract_ome_xml(file)
        except Exception as e:
            return str(e)
        if physicalsize_errors := self.check_physicalsize_fields(
            file, strip_namespace_and_parse(extracted_ome_xml)
        ):
            return physicalsize_errors

    def check_physicalsize_fields(self, filename: Path, xml_etree: ET.Element) -> str | None:
        """
        Compare PhysicalSizeX / Y / Z values in OME XML images against
        values in self.maximums.
        """
        errors = []
        # This uses the first Image element found; multiple Image elements are possible
        # in a single OME-TIFF but the first *should* be the full-sized image.
        if (xml_image_data := xml_etree.find("Image/Pixels")) is None:
            return f"No Image/Pixels found in file {self.rel_filename_str(filename)}"
        for coordinate in ["X", "Y", "Z"]:
            if not (max := self.maximums.get(coordinate)):
                continue
            try:
                self.check_coordinate(self.get_size_data(xml_image_data, coordinate), max)
            except Exception as e:
                errors.append(str(e))
        if errors:
            error_str = (
                f"{self.rel_filename_str(filename)} OME-XML errors: {'; '.join(sorted(errors))}"
            )
            return error_str

    def get_size_data(self, xml_image_data: ET.Element, coordinate: str) -> SizeData:
        errors = []
        sizes = {}
        required_keys = {
            f"PhysicalSize{coordinate}": "physicalsize",
            f"Size{coordinate}": "size",
        }
        for coord_key, generic_key in required_keys.items():
            if not (value := xml_image_data.get(coord_key)):
                errors.append(f"{coord_key} missing")
            else:
                sizes[generic_key] = value
        sizes["unit"] = xml_image_data.get(f"PhysicalSize{coordinate}Unit", "µm")
        if errors:
            raise Exception(", ".join(errors))
        return SizeData(**sizes, coordinate=coordinate)

    def check_coordinate(self, size_data: SizeData, max: float):
        """
        Ensure that a given coordinate's PhysicalSize converts to micrometers,
        Size is a float, and PhysicalSize * Size is less than the specified maximum.

        Args:
            sizes: SizeData object for coordinate (PhysicalSize, Size, and PhysicalSizeUnit)
            max: maximum value for comparison

        Return None on success; otherwise raise.
        """
        physicalsize = size_data.physicalsize
        key = f"PhysicalSize{size_data.coordinate}"
        try:
            size = float(size_data.size)
        except ValueError as e:
            raise Exception(
                f"{key}Unit '{size_data.size}' type is {type(size_data.size).__name__}"
            ) from e
        try:
            micrometer_value = convert_to_micrometers(float(physicalsize), size_data.unit)
        except ValueError as e:
            raise Exception(f"{key} '{physicalsize}' type is {type(physicalsize).__name__}") from e
        except (DimensionalityError, UndefinedUnitError) as e:
            raise Exception(f"Error in unit parsing or conversion for {key}: {e}") from e
        except Exception as e:
            raise Exception(f"Error with {key}: {e}") from e
        if (micrometer_value * size) > max:
            raise Exception(
                f"{key} {physicalsize} {size_data.unit} * Size{size_data.coordinate} {size} is greater than maximum value {max} µm"
            )


def convert_to_micrometers(value: float, units: str) -> float:
    ureg = UnitRegistry(system="SI")
    q = ureg.Quantity(value, units)
    return float(q.to("micrometers").magnitude)
