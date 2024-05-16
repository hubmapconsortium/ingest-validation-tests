import re
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        ("test_data/publication_tree_good.zip", [None], "Publication"),
        ("test_data/publication_tree_good_complex.zip", [None], "Publication"),
        (
            "test_data/publication_tree_bad_complex.zip",
            [
                "expected data file data/vignette_12/A/0/325b936e-4132-45fe-8674-9abbde568be8 is absent",  # noqa: E501
                "expected data file data/vignette_12/A/0/9db02302-07d9-4c54-ad45-4578c4822cce is absent",  # noqa: E501
                "expected data file data/vignette_12/A/1/90b3667d-3ccc-4241-9227-fee578d41bac is absent",  # noqa: E501
            ],
            "Publication"
        ),
        ("test_data/publication_tree_bad_1.zip", ["vignettes not found or not a directory"], "Publication"),
        ("test_data/publication_tree_bad_2.zip", ["Found a subdirectory in a vignette"], "Publication"),
        ("test_data/publication_tree_bad_3.zip", ["A vignette has more than one markdown file"], "Publication"),
        ("test_data/publication_tree_bad_4.zip", ["figure dict does not provide a name"], "Publication"),
        ("test_data/publication_tree_bad_5.zip", ["figure dict does not reference a file"], "Publication"),
        ("test_data/publication_tree_bad_6.zip", ["unexpected files in vignette.*"], "Publication"),
        (
            "test_data/publication_tree_bad_7.zip",
            [
                "expected data file"
                " data/codeluppi_2018_nature_methods.molecules.h5ad.zarr"
                " is absent"
            ],
            "Publication",
        ),
        ("test_data/tiff_tree_good.zip", [], "codex"),
    ),
)
def test_publication_validator(test_data_fname, msg_re_list, assay_type, tmp_path):
    from publication_validator import PublicationValidator

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = PublicationValidator(tmp_path / test_data_path.stem, assay_type)
    errors = validator.collect_errors(coreuse=4)[:]
    print(f"errors: {errors}")
    matched_err_str_list = []
    for err_str in errors:
        for re_str in msg_re_list:
            if ((err_str is None and re_str is None)
                or re.match(re_str, err_str)):
                msg_re_list.remove(re_str)
                matched_err_str_list.append(err_str)
                break
    print(f"matched errors: {matched_err_str_list}")
    matched_err_str_set = set(matched_err_str_list)
    for err_str in errors:
        assert err_str in matched_err_str_set, f"Unexpected error msg {err_str}"
    assert not msg_re_list, f"Expected error regexes were not matched: {msg_re_list}"
