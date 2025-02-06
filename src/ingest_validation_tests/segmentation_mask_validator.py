import logging
from csv import DictReader
from pathlib import Path
from typing import Optional

import requests
from ingest_validation_tools.plugin_validator import Validator
from requests.exceptions import HTTPError


class SegmentationMaskValidator(Validator):
    """
    Send Object by Feature XLSX file(s) to the Metadata Center validator.
    """

    description = "Test Object by Feature table(s)"
    cost = 1.0
    version = "1.0"
    required = "segmentation-mask"

    # TODO: break up, write tests
    def collect_errors(self, **kwargs) -> list[Optional[str]]:
        """
        Return the errors found by this validator
        """
        del kwargs
        if self.required not in self.contains and self.assay_type.lower() != self.required:
            return []  # We only test segmentation-mask data
        rslt_list = []
        xlsx_files = []
        suffix = "*.[xX][lL][sS][xX]"
        # TODO: make file finding smarter;
        # pattern from dir-schema: derived\/segmentation_masks\/[^\/]+-objects\.(?:tsv|xlsx)$
        for path in self.paths:
            for file in path.glob(suffix):
                xlsx_files.append(file)
        object_x_feature_files = []
        for file in xlsx_files:
            rows = read_rows(file)
            if rows[0].get("Type"):
                object_x_feature_files.append(file)
        for file_path in object_x_feature_files:
            with open(file_path, "rb") as f:
                file = {"input_file": f}
                headers = {"content_type": "multipart/form-data"}
                try:
                    response = requests.post(
                        # TODO: stage?
                        "https://api.stage.metadatavalidator.metadatacenter.org/service/validate-structured-xlsx",
                        headers=headers,
                        files=file,
                    )
                    logging.info(f"Response: {response.json()}")
                    response.raise_for_status()
                except HTTPError as e:
                    rslt_list.append(f"Error while checking {file}: {e}")
        if rslt_list:
            return rslt_list
        elif object_x_feature_files:
            return [None]
        else:
            return []


def read_rows(path: Path) -> list:
    if not Path(path).exists():
        raise Exception(f"File does not exist: {path}")
    try:
        rows = dict_reader_wrapper(path)
        if not rows:
            raise Exception(f"File has no data rows: {path}")
        else:
            return rows
    except Exception as e:
        raise Exception(f"Failed to read file {path}. Error: {e}")


def dict_reader_wrapper(path) -> list:
    with open(path) as f:
        rows = list(DictReader(f, dialect="excel-tab"))
        f.close()
    return rows
