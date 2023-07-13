from pathlib import Path
import zipfile
import re

import pytest

@pytest.mark.parametrize(('test_data_fname', 'msg_re_list'), (
    ('test_data/publication_tree_good.zip', []),
    # ('test_data/tiff_tree_bad.zip', [
    #     '.*notatiff.tif is not a valid TIFF file',
    #     '.*notatiff.tiff is not a valid TIFF file',
    #     '.*notatiff.TIFF is not a valid TIFF file',
    #     '.*notatiff.TIF is not a valid TIFF file',
    # ]),
    ))
def test_tiff_validator(test_data_fname, msg_re_list, tmp_path):
    from publication_validator import PublicationValidator
    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = PublicationValidator(tmp_path / test_data_path.stem, 'Publication')
    errors = validator.collect_errors(coreuse=4)[:]
    print(f"errors: {errors}")
    matched_err_str_list = []
    for err_str in errors:
        for re_str in msg_re_list:
            if re.match(re_str, err_str):
                msg_re_list.remove(re_str)
                matched_err_str_list.append(err_str)
                break
    print(f"matched errors: {matched_err_str_list}")
    matched_err_str_set = set(matched_err_str_list)
    for err_str in errors:
        assert err_str in matched_err_str_set, f"Unexpected error msg {err_str}"
