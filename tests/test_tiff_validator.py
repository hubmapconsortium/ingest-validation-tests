from pathlib import Path
import zipfile
import re

import pytest

@pytest.mark.parametrize(('test_data_fname', 'msg_re_list'), (
    ('test_data/tiff_tree_good.zip', []),
    ('test_data/tiff_tree_bad.zip', [
        '.*notatiff.tif is not a valid TIFF file',
        '.*notatiff.tiff is not a valid TIFF file',
        '.*notatiff.TIFF is not a valid TIFF file',
        '.*notatiff.TIF is not a valid TIFF file',
    ]),
    ))
def test_tiff_validator(test_data_fname, msg_re_list, tmp_path):
    from tiff_validator import TiffValidator
    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = TiffValidator(tmp_path / test_data_path.stem, 'codex')
    errors = validator.collect_errors(coreuse=4)[:]
    print(f"errors: {errors}")
    assert len(msg_re_list) == len(errors)
    for err_str, re_str in zip(errors, msg_re_list):
        assert re.match(re_str, err_str, flags=re.MULTILINE)
