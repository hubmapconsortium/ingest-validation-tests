import logging
from functools import cached_property
from pathlib import Path
from typing import Optional, Union

import requests
from ingest_validation_tools.plugin_validator import Validator
from requests.exceptions import HTTPError


class SegmentationMaskValidator(Validator):
    """
    Send Object by Feature XLSX file(s) to the Metadata Center validator.
    This plugin assumes that at least one XLSX file is present in an upload
    with dataset type segmentation mask.
    """

    description = "Test Object by Feature table(s)"
    cost = 1.0
    version = "1.0"
    required = ["segmentation mask"]

    def collect_errors(self, **kwargs) -> list[Optional[Union[str, None]]]:
        """
        Return the errors found by this validator.

        Return types:
            list[str]: errors reported, report plugin run
            list[None]: no errors reported, report plugin run
            list[]: plugin not relevant to this dataset type, don't report plugin run
        """
        del kwargs
        print("Checking relevance of SegmentationMaskValidator...")
        required_type = False
        for dataset_type in self.required:
            if dataset_type not in self.contains and self.assay_type.lower() != dataset_type:
                continue
            else:
                required_type = True
        if not required_type:
            return []  # We only test segmentation mask data
        print("Running SegmentationMaskValidator...")
        if not self.xlsx_files_list:
            return ["No object by feature .XLSX files found."]
        rslt_list = [self.validate_file(file_path) for file_path in self.xlsx_files_list]
        if rslt_list:
            return rslt_list
        else:
            return [None]

    @cached_property
    def xlsx_files_list(self) -> list[Path]:
        # Requires lowercase; can use glob case_sensitive arg when upgraded to Python 3.12
        xlsx_files = []
        suffix = "*-objects.xlsx"
        for path in self.paths:
            expected_path = path.joinpath("derived/segmentation_masks/")
            if expected_path.exists():
                for file in expected_path.glob(suffix):
                    xlsx_files.append(file)
        return xlsx_files

    def validate_file(self, file_path: Path) -> Optional[str]:
        with open(file_path, "rb") as f:
            file = {"input_file": f}
            headers = {"content_type": "multipart/form-data"}
            response = requests.post(
                # TODO: stage?
                "https://api.stage.metadatavalidator.metadatacenter.org/service/validate-structured-xlsx",
                headers=headers,
                files=file,
            )
            logging.info(f"Response: {response.json()}")
            try:
                response.raise_for_status()
            except HTTPError:
                message = response.json().get("message", "")
                cause = response.json().get("cause", "")
                fixSuggestion = response.json().get("fixSuggestion", "")
                return (
                    f"Error while checking file {file_path.stem} because of error '{message}'. "
                    f"Cause: {cause} "
                    f"Suggestion: {fixSuggestion}"
                )
