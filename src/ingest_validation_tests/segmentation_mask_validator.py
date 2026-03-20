import logging
from functools import cached_property
from json.decoder import JSONDecodeError
from pathlib import Path

import requests
from requests.exceptions import HTTPError
from validator import Validator


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

    def _collect_errors(self) -> list[str | None]:
        if not self.xlsx_files_list:
            return ["No object by feature .XLSX files found."]
        rslt_list = [self.validate_file(file_path) for file_path in self.xlsx_files_list]
        flat_list = []
        for subitem in rslt_list:
            if type(subitem) is list:
                flat_list.extend(subitem)
            else:
                flat_list.append(subitem)
        return self._return_result(flat_list, self.xlsx_files_list)

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

    def validate_file(self, file_path: Path) -> str | list[str] | None:
        with open(file_path, "rb") as f:
            file = {"input_file": f}
            headers = {"content-type": "multipart/form-data"}
            response = requests.post(
                "https://api.stage.metadatavalidator.metadatacenter.org/service/validate-structured-xlsx",
                headers=headers,
                files=file,
            )
            try:
                logging.info(f"Response: {response.json()}")
                response.raise_for_status()
            except JSONDecodeError:
                logging.info(f"Response: {response.text}")
                return f"""
                    Error while checking file {self.rel_filename_str(file_path)}.
                    {response.text}
                """
            except HTTPError:
                # File missing header, etc.
                message = response.json().get("message", "")
                cause = response.json().get("cause", "")
                fixSuggestion = response.json().get("fixSuggestion", "")
                return (
                    f"Error while checking file {self.rel_filename_str(file_path)}: {message}. "
                    f"Cause: {cause} "
                    f"Suggestion: {fixSuggestion}"
                )
            # Actual validation errors
            if errors := response.json().get("reporting"):
                error_strs = []
                for error in errors:
                    row = error.get("row")
                    col = error.get("column")
                    val = error.get("value")
                    err_type = error.get("errorType")
                    msg = error.get("errorMessage")
                    repair = error.get("repairSuggestion")
                    # 8 rows of header info in object x feature XLSX template
                    err_str = f"{self.rel_filename_str(file_path)}: Row {row + 10}, column '{col}', value '{val}': {msg} (error type: {err_type})."
                    if repair and repair != "Not applicable":
                        err_str += f" Repair suggestion: {repair}."
                    error_strs.append(err_str)
                return error_strs
