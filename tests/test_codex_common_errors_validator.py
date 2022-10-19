import pytest
import sys
from pathlib import Path
import zipfile

@pytest.mark.parametrize(('test_data_fname', 'msg_starts_list'), (
    ('test_data/fake_codex_tree_0.zip', ['Unexpected error reading']),
    ('test_data/fake_codex_tree_1.zip', ['The segmentation.json file is in',
                                         'Unexpected error reading']),
    ('test_data/fake_codex_tree_2.zip', ['The raw/src_ subdirectory is missing?']),
    ('test_data/fake_codex_tree_3.zip', ['channelnames.txt and/or channelnames_report.csv is missing']),
    ('test_data/fake_codex_tree_4.zip', ['channelnames.txt and/or channelnames_report.csv is missing']),
    ('test_data/fake_codex_tree_5.zip', ['channelnames.txt does not match channelnames_report.txt on line 1: HLADR vs HLA-DR',
                                         'channelnames.txt does not match channelnames_report.txt on line 6: Empty vs Blank']),
    ('test_data/fake_codex_tree_6.zip', ['Could not parse ']),
    ('test_data/fake_codex_tree_7.zip', []),
    ))
def test_codex_common_errors_validator(test_data_fname, msg_starts_list, tmp_path):
    from codex_common_errors_validator import CodexCommonErrorsValidator
    test_data_path = Path(test_data_fname)
    zf = zipfile.ZipFile(test_data_path)
    zf.extractall(tmp_path)
    validator = CodexCommonErrorsValidator(tmp_path / test_data_path.stem,
                                           'CODEX'
                                           )
    errors = validator.collect_errors()[:]
    assert len(msg_starts_list) == len(errors)
    for err_str, expected_str in zip(errors, msg_starts_list):
        assert err_str.startswith(expected_str)


