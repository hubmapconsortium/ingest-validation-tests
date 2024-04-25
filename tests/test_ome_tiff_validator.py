import re
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list"),
    (
        (
            "test_data/codex_tree_ometiff_bad.zip",
            [".*tubhiswt_C0_bad.ome.tif is not a valid OME.TIFF file.*"],
        ),
        ("test_data/codex_tree_ometiff_good.zip", []),
    ),
)
def test_ome_tiff_validator(test_data_fname, msg_re_list, tmp_path):
    from ome_tiff_validator import OmeTiffValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = OmeTiffValidator(tmp_path / test_data_path.stem, "CODEX")
    errors = validator.collect_errors(coreuse=4)[:]
    assert len(msg_re_list) == len(errors)
    for err_str, re_str in zip(errors, msg_re_list):
        assert re.match(re_str, err_str, flags=re.MULTILINE)
