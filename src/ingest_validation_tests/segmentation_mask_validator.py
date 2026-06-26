import logging
from collections import Counter
from functools import cached_property
from json.decoder import JSONDecodeError
from pathlib import Path

import pandas as pd
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
        rslt_list = []
        for file_path in self.xlsx_files_list:
            rslt_list.extend(self.check_duplicate_object_ids(file_path))
            result = self.validate_file(file_path)
            if type(result) is list:
                rslt_list.extend(result)
            else:
                rslt_list.append(result)
        return self._return_result(rslt_list, self.xlsx_files_list)

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

    def check_duplicate_object_ids(self, file_path: Path) -> list[str]:
        try:
            raw = pd.read_excel(file_path, header=None)
            all_na = raw.isnull().all(axis=1)
            na_indices = all_na[all_na].index.tolist()
            if not na_indices:
                return [f"Could not find header row in {file_path}: no all-NA row found."]
            header_row = na_indices[0] + 2
            df = pd.read_excel(file_path, header=header_row)
        except Exception as e:
            return [f"Could not check duplicate Object IDs in {file_path}: {e}"]
        if "Object ID" not in df.columns:
            return [f"Could not find Object ID in {file_path}."]
        object_ids = df["Object ID"].dropna()
        counts = Counter(object_ids)
        duplicates = {obj_id: count for obj_id, count in counts.items() if count > 1}
        if duplicates:
            dup_strs = [f"{obj_id} ({count} occurrences)" for obj_id, count in duplicates.items()]
            return [
                f"{self.rel_filename_str(file_path)}: "
                f"Found duplicate Object IDs: {', '.join(dup_strs)}"
            ]
        return []

    def validate_file(self, file_path: Path) -> str | list[str] | None:
        with open(file_path, "rb") as f:
            file = {"input_file": f}
            response = requests.post(
                "https://api.stage.metadatavalidator.metadatacenter.org/service/validate-structured-xlsx",
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
