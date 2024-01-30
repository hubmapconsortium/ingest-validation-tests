from pathlib import Path
import zipfile

import pytest

@pytest.mark.parametrize(('test_data_fname', 'msg_starts_list'), (
    ('test_data/fake_codex_tree_0.zip', ['Unexpected error reading']),
    ('test_data/fake_codex_tree_1.zip', ['The segmentation.json file is in',
                                         'Unexpected error reading']),
    ('test_data/fake_codex_tree_2.zip', ['The raw/src_ subdirectory is missing?']),
    ('test_data/fake_codex_tree_3.zip', ['channelnames.txt is missing']),
    ('test_data/fake_codex_tree_4.zip', ['Unexpected error reading']),
    ('test_data/fake_codex_tree_5.zip', ['channelnames.txt does not match channelnames_report.txt'
                                         ' on line 1: HLADR vs HLA-DR',
                                         'channelnames.txt does not match channelnames_report.txt'
                                         ' on line 6: Empty vs Blank']),
    ('test_data/fake_codex_tree_6.zip', ['Could not parse ']),
    ('test_data/fake_codex_tree_7.zip', []),
    ('test_data/fake_codex_tree_8.zip', ['Region numbers are not contiguous']),
    ('test_data/fake_codex_tree_9.zip', ['Cycle numbers are not contiguous',
                                         'The number of channels per cycle is not constant']),
    ('test_data/fake_codex_tree_10.zip', ['Directory string "cyc0a3_reg001_211119_040351"'
                                          ' cycle number is not an integer']),
    ('test_data/fake_codex_tree_11.zip', ['Directory string "cyc003_reg0a1_211119_040351"'
                                          ' region number is not an integer']),
    ('test_data/fake_codex_tree_12.zip', ['Directory string "cyc002_rig001_211119_040351"'
                                          ' does not include "_reg"']),
    ('test_data/fake_codex_tree_13.zip', ['Cycle numbering does not start at 1']),
    ('test_data/fake_codex_tree_14.zip', ['Region numbering does not start at 1']),
    ('test_data/fake_codex_tree_15.zip', ['Not all cycle/region pairs are present',
                                          'The number of channels per cycle is not constant']),
    ('test_data/fake_codex_tree_16.zip', []),
    ('test_data/fake_codex_tree_17.zip', ['A dataset.json file exists but is in the wrong place',
                                          'Region numbering does not start at 1']),
    ('test_data/fake_codex_tree_18.zip', ['The number of channels per cycle is not constant']),
    ('test_data/fake_codex_tree_19.zip', []),
    ))
def test_codex_common_errors_validator(test_data_fname, msg_starts_list, tmp_path):
    from codex_common_errors_validator import CodexCommonErrorsValidator
    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = CodexCommonErrorsValidator([Path(tmp_path / test_data_path.stem)],
                                           'CODEX'
                                           )
    errors = validator.collect_errors()[:]
    print(f'ERRORS FOLLOW FOR {test_data_fname}')
    for err in errors:
        print(err)
    print('ERRORS ABOVE')
    assert len(msg_starts_list) == len(errors)
    for err_str, expected_str in zip(errors, msg_starts_list):
        assert err_str.startswith(expected_str)
