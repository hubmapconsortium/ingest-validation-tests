import re
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        ("test_data/codex_tree_ometiff_bad.zip",
         [
             ".*tubhiswt_C0_bad.ome.tif is not a valid OME.TIFF file.*",
             ".*sample1.ome.tif is not a valid OME.TIFF file.*",
             ".*sample2.ome.tif is not a valid OME.TIFF file.*",
         ],
         "CODEX"),
        ("test_data/codex_tree_ometiff_good.zip",
         [],
         "CODEX"),
        ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
    ),
)
def test_ome_tiff_field_validator(test_data_fname, msg_re_list, assay_type, tmp_path):
    from ome_tiff_field_validator import OmeTiffFieldValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = OmeTiffFieldValidator(tmp_path / test_data_path.stem, assay_type)
    errors = validator.collect_errors(coreuse=4)[:]
    errors = list(err for err in errors if err is not None)
    assert len(msg_re_list) == len(errors)
    unmatched_errors = []
    for err_str in errors:
        msg_re_list_dup = list(msg_re_list) # to avoid editing during iteration
        match = False
        for re_str in msg_re_list_dup:
            if ((err_str is None and re_str is None)
                or re.match(re_str, err_str, flags=re.MULTILINE)):
                msg_re_list.remove(re_str)
                match = True
                break
        if not match:
            unmatched_errors.append(err_str)
    assert not unmatched_errors, f"Unmatched errors: {unmatched_errors}"
