from pathlib import Path

from tests_utils import GetParentData
from validator import Validator, check_ome_tiff_file


def get_ometiff_sizes(file) -> str | list[dict]:
    try:
        try:
            xml_document = check_ome_tiff_file(file)
        except Exception as e:
            return str(e)
        images = xml_document.schema.to_dict(xml_document).get("Image")  # type: ignore | xmlschema.DecodeType causing issues
    except Exception as e:
        return f"{file} is not a valid OME.TIFF file: {e}"
    try:
        sizes = []
        for image in images:
            xml_image_data = image.get("Pixels")
            sizes.append(
                {
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
            )
        return sizes
    except Exception as excp:
        return f"{file} is not a valid OME.TIFF file: {excp}"


class ImageSizeValidator(Validator):
    description = "Check dataset and parent image size so they can be matched in the visualization"
    cost = 1.0
    version = "1.0"
    required = ["segmentation mask"]
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

    def _collect_errors(self) -> list[str | None]:
        if not self.schema_rows:
            return ["No metadata rows found."]
        files_tested = False
        output = []
        for i, row in enumerate(self.schema_rows):
            filenames_to_test = []
            parent_filenames_to_test = []
            try:
                md_data_path = Path(row["data_path"])

                data_path = Path(self.paths[0]) / md_data_path
                if not data_path.is_dir():
                    data_path = Path(self.paths[0].parent) / md_data_path

                assert (data_path.is_dir()), f"Data path does not exist ({data_path})"

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

                assert (
                    len(filenames_to_test) == 1
                ), f"Too many or too few files Mask ({[self.rel_filename_str(path) for path in filenames_to_test]})"
                assert (
                    len(parent_filenames_to_test) == 1
                ), f"Too many or too few files Base Images ({[self.rel_filename_str(path) for path in parent_filenames_to_test]})"

                segmentation_mask_sizes = get_ometiff_sizes(filenames_to_test[0])
                base_image_sizes = get_ometiff_sizes(parent_filenames_to_test[0])
                assert not isinstance(segmentation_mask_sizes, str), segmentation_mask_sizes
                assert not isinstance(base_image_sizes, str), base_image_sizes
                assert any(
                    mask_size in base_image_sizes for mask_size in segmentation_mask_sizes
                ), "No segmentation mask image matches any base image size"
                files_tested = True

            except AssertionError as e:
                output.append(str(e))
        return self._return_result(output, files_tested)