import zipfile
from pathlib import Path

import pytest
from publication_vignettes_validator import (
    PublicationVignettesValidator,
)


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        # good submission
        ("test_data/publication_tree_good.zip", [None], "Publication"),
        ("test_data/publication_tree_good_complex.zip", [None], "Publication"),
        # no directory
        (
            "test_data/publication_tree_bad_1.zip",
            [
                {
                    "publication_tree_bad_1": "Directory not found.",
                }
            ],
            "Publication",
        ),
        # data_path / vignettes is not a dir
        (
            "test_data/publication_tree_bad_7.zip",
            [
                {
                    "publication_tree_bad_7": "publication_tree_bad_7/vignettes is not a directory.",
                }
            ],
            "Publication",
        ),
        # vignette path is not a directory
        (
            "test_data/publication_tree_bad_4.zip",
            [{"publication_tree_bad_4/vignettes/does_not_belong_here": "Not a directory."}],
            "Publication",
        ),
        # subdirectories in vignette dir
        (
            "test_data/publication_tree_bad_2.zip",
            [
                {
                    "publication_tree_bad_2/vignettes/vignette_01": [
                        "Found subdirectories in vignette: publication_tree_bad_2/vignettes/vignette_01/thisisasubdirectory"
                    ]
                }
            ],
            "Publication",
        ),
        # extra file in vignette dir
        (
            "test_data/publication_tree_bad_6.zip",
            [
                {
                    "publication_tree_bad_6/vignettes/vignette_01": [
                        "Unexpected files in vignette: publication_tree_bad_6/vignettes/vignette_01/randomfile.txt"
                    ]
                }
            ],
            "Publication",
        ),
        # multiple markdown files
        (
            "test_data/publication_tree_bad_3.zip",
            [
                {
                    "publication_tree_bad_3/vignettes/vignette_01": [
                        "Vignette has more than one markdown file: description.md, description2.md"
                    ]
                }
            ],
            "Publication",
        ),
        # test .md: missing files
        (
            "test_data/publication_tree_bad_complex.zip",
            [
                {
                    "publication_tree_bad_complex/vignettes/vignette_12": [
                        {
                            "description.md": [
                                "Expected data file data/vignette_12/A/0/325b936e-4132-45fe-8674-9abbde568be8 is absent.",
                                "Expected data file data/vignette_12/A/0/9db02302-07d9-4c54-ad45-4578c4822cce is absent.",
                                "Expected data file data/vignette_12/A/1/90b3667d-3ccc-4241-9227-fee578d41bac is absent.",
                            ],
                        }
                    ]
                }
            ],
            "Publication",
        ),
        # wrong data type
        ("test_data/tiff_tree_good.zip", [], "codex"),
    ),
)
def test_publication_vignettes_validator(test_data_fname, msg_re_list, assay_type, tmp_path):

    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    validator = PublicationVignettesValidator(
        tmp_path / test_data_path.stem, assay_type, coreuse=4
    )
    errors = validator.collect_errors()[:]
    print(f"errors: {errors}")
    assert errors == msg_re_list


@pytest.mark.parametrize(
    ("filename", "expected_errors"),
    (
        # good example
        (
            "test_data/publication_vignette_good.md",
            ({}, set()),
        ),
        # missing top-level key
        (
            "test_data/publication_vignette_bad_missing_top_level_key.md",
            (
                {
                    "publication_vignette_bad_missing_top_level_key.md": [
                        "Vignette markdown is incorrectly formatted. Missing required element 'name'.",
                    ],
                },
                set(),
            ),
        ),
        # missing key in figures dict
        (
            "test_data/publication_vignette_bad_missing_top_level_key.md",
            (
                {
                    "publication_vignette_bad_missing_top_level_key.md": [
                        "Vignette markdown is incorrectly formatted. Missing required element 'name'.",
                    ]
                },
                set(),
            ),
        ),
    ),
)
def test_md_files(filename, expected_errors, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(
            "publication_vignettes_validator.PublicationVignettesValidator.validate_vitessce_config",
            lambda a, b, c: [],
        )
        validator = PublicationVignettesValidator("test_path", "Publication")
        errors = validator._check_vignette_md(
            Path(filename),
            {
                Path(filename),
                Path("test_data/doesnt_matter.json"),
            },  # json file referenced in .md file
            Path("test_data"),
            Path("test_data"),
        )
        assert errors == expected_errors
