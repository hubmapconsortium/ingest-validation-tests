import zipfile
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest
import requests


@pytest.mark.parametrize(
    ("test_data_fname", "success", "response_data", "expected_errors", "assay_type"),
    (
        (
            "test_data/seg_mask_good.zip",
            True,
            b'{"status":"PASSED","reporting":[]}',
            [None],
            "Segmentation Mask",
        ),
        (
            "test_data/seg_mask_empty.zip",
            False,
            "",
            ["No object by feature .XLSX files found."],
            "Segmentation Mask",
        ),
        (
            "test_data/seg_mask_bad.zip",
            False,
            b'{"message":"Bad Excel file","cause":"Missing a separator row.","statusInfo":"400 Bad Request","fixSuggestion":"Please add a new blank row between the schema table and the data table to separate."}',
            [
                "Error while checking file seg_mask_bad-objects because of error 'Bad Excel file'. Cause: Missing a separator row. Suggestion: Please add a new blank row between the schema table and the data table to separate."
            ],
            "Segmentation Mask",
        ),
        (
            "test_data/seg_mask_bad.zip",
            None,
            "",
            [],
            "snRNAseq",
        ),
    ),
)
def test_segmentation_mask_validator(
    test_data_fname, success, response_data, expected_errors, assay_type, tmp_path
):
    from segmentation_mask_validator import SegmentationMaskValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = SegmentationMaskValidator(tmp_path / test_data_path.stem, assay_type)
    with patch("segmentation_mask_validator.requests.post") as mock_api_call:
        mock_api_call.return_value = get_mock_response(success, response_data)
        errors = validator.collect_errors()
        print(f"errors: {errors}")
        assert errors == expected_errors


def get_mock_response(success: Optional[bool], response_data: bytes):
    mock_resp = requests.models.Response()
    mock_resp.status_code = 200 if success else 400
    mock_resp._content = response_data
    return mock_resp
