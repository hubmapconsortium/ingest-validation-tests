from pathlib import Path
from typing import List, Optional, Union

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator  # type: ignore
from tests_utils import GetParentData


def get_ometiff_size(file) -> Union[str, dict]:
    try:
        tf = tifffile.TiffFile(file)
        xml_document = xmlschema.XmlDocument(tf.ome_metadata)  # type: ignore | checks below should be sufficient if bad type returned
        if xml_document.schema and not xml_document.schema.is_valid(xml_document):
            return f"{file} is not a valid OME.TIFF file."
        elif not xml_document.schema:
            return f"Can't read OME XML from file {file}."
        xml_image_data = (
            xml_document.schema.to_dict(xml_document).get("Image")[0].get("Pixels")  # type: ignore | xmlschema.DecodeType causing issues
        )
    except Exception as excp:
        return f"{file} is not a valid OME.TIFF file: {excp}"
    try:
        rst = {
            "X": xml_image_data.get("@PhysicalSizeX"),
            "XUnits": xml_image_data.get("@PhysicalSizeXUnit"),
            "XPix": xml_image_data.get("@SizeX"),
            "Y": xml_image_data.get("@PhysicalSizeY"),
            "YUnits": xml_image_data.get("@PhysicalSizeYUnit"),
            "YPix": xml_image_data.get("@SizeY"),
            "Z": xml_image_data.get("@PhysicalSizeZ"),
            "ZUnits": xml_image_data.get("@PhysicalSizeZUnit"),
            "ZPix": xml_image_data.get("@SizeZ"),
        }
        return rst
    except Exception as excp:
        return f"{file} is not a valid OME.TIFF file: {excp}"


class ImageSizeValidator(Validator):
    description = "Check dataset and parent image size so they can be matched in the visualization"
    cost = 1.0
    version = "1.0"
    required = "segmentation mask"
    files_to_find = [
        "**/segmentation_masks/*.ome.tif",
        "**/segmentation_masks/*.ome.tiff",
        "**/segmentation_masks/*.OME.TIFF",
        "**/segmentation_masks/*.OME.TIF",
    ]
    parent_files_to_find = [
        "**/*.ome.tif",
        "**/*.ome.tiff",
        "**/*.OME.TIFF",
        "**/*.OME.TIF",
    ]

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        del kwargs
        print("Validating Image/SegMask sizes")
        if self.required not in self.contains and self.assay_type.lower() != self.required:
            return []  # We only test Segmentation Masks
        files_tested = None
        output = []
        filenames_to_test = []
        parent_filenames_to_test = []
        try:
            for row in self.schema.rows:
                data_path = Path(row["data_path"])
                if not data_path.is_absolute():
                    data_path = Path(self.paths[0]).parent / data_path

                for glob_expr in self.files_to_find:
                    for file in data_path.glob(glob_expr):
                        filenames_to_test.append(file)

                for glob_expr in self.parent_files_to_find:
                    for file in Path(
                        GetParentData(
                            row["parent_dataset_id"], self.token, self.app_context
                        ).get_path()
                    ).glob(glob_expr):
                        parent_filenames_to_test.append(file)

                assert len(filenames_to_test) == 1, "Too many or too few files Mask"
                assert len(parent_filenames_to_test) == 1, "Too many or too few files Base Images"

                segmentation_mask_size = get_ometiff_size(filenames_to_test[0])
                base_image_size = get_ometiff_size(parent_filenames_to_test[0])
                assert (
                    segmentation_mask_size == base_image_size
                ), "Files and base image size do not match"

        except AssertionError as exep:
            output.append(str(exep))

        if output:
            return output
        elif files_tested:
            return [None]
        else:
            return []
