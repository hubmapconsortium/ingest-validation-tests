import re
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        ("test_data/tiff_tree_good.zip", [None], "codex"),
        (
            "test_data/tiff_tree_bad.zip",
            [
                ".*notatiff.tif is not a valid TIFF file",
                ".*notatiff.tiff is not a valid TIFF file",
                ".*notatiff.TIFF is not a valid TIFF file",
                ".*notatiff.TIF is not a valid TIFF file",
            ],
            "codex",
        ),
        ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
    ),
)
def test_tiff_validator(test_data_fname, msg_re_list, assay_type, tmp_path):
    from tiff_validator import TiffValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = TiffValidator(tmp_path / test_data_path.stem, assay_type)
    errors = validator.collect_errors(coreuse=4)[:]
    print(f"errors: {errors}")
    matched_err_str_list = []
    for err_str in errors:
        for re_str in msg_re_list:
            if (re_str is None and err_str is None) or (re.match(re_str, err_str)):
                msg_re_list.remove(re_str)
                matched_err_str_list.append(err_str)
                break
    print(f"matched errors: {matched_err_str_list}")
    matched_err_str_set = set(matched_err_str_list)
    for err_str in errors:
        assert err_str in matched_err_str_set, f"Unexpected error msg {err_str}"
