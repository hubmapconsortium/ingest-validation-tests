from pathlib import Path
import zipfile
import re

import pytest

@pytest.mark.parametrize(('test_data_fname', 'msg_re_list'), (
    ('test_data/publication_tree_good.zip', []),
    ('test_data/publication_tree_bad_1.zip', ['vignettes not found or not a directory']),
    ('test_data/publication_tree_bad_2.zip', ['Found a subdirectory in a vignette']),
    ('test_data/publication_tree_bad_3.zip', ['A vignette has more than one markdown file']),
    ('test_data/publication_tree_bad_4.zip', ['figure dict does not provide a name']),
    ('test_data/publication_tree_bad_5.zip', ['figure dict does not reference a file']),
    ('test_data/publication_tree_bad_6.zip', ['unexpected files in vignette.*']),
    ))
def test_publication_validator(test_data_fname, msg_re_list, tmp_path):
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
    assert not msg_re_list, f"Expected error regexes were not matched: {msg_re_list}"
