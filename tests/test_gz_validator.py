import re
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        ("test_data/fake_snrnaseq_tree_good.zip", [None], "snRNAseq"),
        (
            "test_data/fake_snrnaseq_tree_bad.zip",
            [".*text2.txt.gz is not a valid gzipped file"],
            "snRNAseq",
        ),
        ("test_data/codex_tree_ometiff_good.zip", [], "CODEX"),
    ),
)
def test_gz_validator(test_data_fname, msg_re_list, assay_type, tmp_path):
    from gz_validator import GZValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = GZValidator(tmp_path / test_data_path.stem, assay_type)
    errors = validator.collect_errors(coreuse=4)[:]
    assert len(msg_re_list) == len(errors)
    for err_str, re_str in zip(errors, msg_re_list):
        assert (err_str is None and re_str is None) or (
            re.match(re_str, err_str, flags=re.MULTILINE)
        )
