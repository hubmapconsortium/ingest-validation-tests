import logging
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

    @property
    def key(self) -> str:
        return f"PhysicalSize{self.coordinate}"

    def __post_init__(self):
        """
        Ensure that Size and PhysicalSize are (castable to) floats.
        """
        try:
            self.size = float(self.size)
        except ValueError as e:
            raise Exception(
                f"Size{self.coordinate} '{self.size}' type is {type(self.size).__name__}"
            ) from e
        try:
            self.physicalsize = float(self.physicalsize)
        except ValueError as e:
            raise Exception(
                f"{self.key} '{self.physicalsize}' type is {type(self.physicalsize).__name__}"
            ) from e

    def converted_value(self, units: str):
        """
        Ensure that a given coordinate's PhysicalSize converts to given unit,
        and return value.
        """
        try:
            ureg = UnitRegistry(system="SI")
            q = ureg.Quantity(self.physicalsize, self.unit)
            return float(q.to(units).magnitude)
        except ValueError as e:
            raise Exception(
                f"{self.key} '{self.physicalsize}' type is {type(self.physicalsize).__name__}"
            ) from e
        except (DimensionalityError, UndefinedUnitError) as e:
            raise Exception(f"Error in unit parsing or conversion for {self.key}: {e}") from e
        except Exception as e:
            raise Exception(f"Error with {self.key}: {e}") from e

    def compare_to_maximum(self, max: float, max_units: str):
        """
        Ensure that PhysicalSize (converted to max_units) * Size is less than the specified maximum.
        """
        if (self.converted_value(max_units) * self.size) > max:
            raise Exception(
                f"{self.key} {self.physicalsize} {self.unit} * Size{self.coordinate} {self.size} is greater than maximum value {max} {max_units}"
            )


class VisiumPhysicalsizeValidator(Validator):
    description = "Test image sizes in Visium OME-TIFF files"
    cost = 2.0
    version = "1.0"
    assay_type = ["visium (with probes)", "visium (no probes)"]

    max_units = "mm"
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
            logging.info(f"Checking {file}...")
            if physicalsize_errors := self.check_physicalsize_fields(
                file, strip_namespace_and_parse(extracted_ome_xml)
            ):
                logging.info(f"Errors found for {file.name}!")
                return physicalsize_errors
        except Exception as e:
            return f"{self.rel_filename_str(file)}: {e}"

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
                coord_data = self.get_size_data(xml_image_data, coordinate)
                coord_data.compare_to_maximum(max, self.max_units)
            except Exception as e:
                errors.append(str(e))
        if errors:
            error_str = (
                f"{self.rel_filename_str(filename)} OME-XML errors: {'; '.join(sorted(errors))}"
            )
            return error_str

    def get_size_data(self, xml_image_data: ET.Element, coordinate: str) -> SizeData:
        """
        Return a SizeData instance with PhysicalSize, Size, and Unit info
        for a given coordinate.
        """
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
