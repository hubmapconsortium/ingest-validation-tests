from pathlib import Path
import zipfile
import re

import pytest

@pytest.mark.parametrize(('test_data_fname', 'msg_re_list'), (
    ('test_data/good_codex_akoya_directory_v1_with_dataset_json_fails.zip',
     [".*is not of type 'object'.*"]),
    ('test_data/good_codex_akoya_directory_v1_with_dataset_json_passes.zip', []),
    ))
def test_codex_json_validator(test_data_fname, msg_re_list, tmp_path):
    from codex_json_validator import CodexJsonValidator
    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = CodexJsonValidator(tmp_path / test_data_path.stem,
                                   'CODEX'
                                   )
    errors = validator.collect_errors()[:]
    print(f'ERRORS FOLLOW FOR {test_data_fname}')
    for err in errors:
        print(err)
    print('ERRORS ABOVE')
    assert len(msg_re_list) == len(errors)
    for err_str, expected_re in zip(errors, msg_re_list):
        assert re.match(expected_re, err_str, flags=re.MULTILINE)
