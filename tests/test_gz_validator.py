import re
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list"),
    (
        ("test_data/fake_snrnaseq_tree_good.zip", []),
        ("test_data/fake_snrnaseq_tree_bad.zip", [".*text2.txt.gz is not a valid gzipped file"]),
    ),
)
def test_gz_validator(test_data_fname, msg_re_list, tmp_path):
    from gz_validator import GZValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = GZValidator(tmp_path / test_data_path.stem, "snRNAseq")
    errors = validator.collect_errors(coreuse=4)[:]
    assert len(msg_re_list) == len(errors)
    for err_str, re_str in zip(errors, msg_re_list):
        assert re.match(re_str, err_str, flags=re.MULTILINE)
