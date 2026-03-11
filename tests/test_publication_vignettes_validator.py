import re
import zipfile
from pathlib import Path

import pytest
from publication_vignettes_validator import (
    PublicationVignettesValidator,
)


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        ("test_data/publication_tree_good.zip", [None], "Publication"),
        ("test_data/publication_tree_good_complex.zip", [None], "Publication"),
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
                            "hint": [PublicationVignettesValidator.hint],
                        }
                    ]
                }
            ],
            "Publication",
        ),
        (
            "test_data/publication_tree_bad_1.zip",
            [
                {
                    "publication_tree_bad_1": "Directory not found.",
                }
            ],
            "Publication",
        ),
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
        (
            "test_data/publication_tree_bad_3.zip",
            [
                {
                    "publication_tree_bad_3/vignettes/vignette_01": [
                        "Vignette has more than one markdown file: description2.md, description.md"
                    ]
                }
            ],
            "Publication",
        ),
        (
            "test_data/publication_tree_bad_4.zip",
            [
                {
                    "publication_tree_bad_4/vignettes/vignette_01": [
                        {
                            "description.md": ["'figure' dict missing required element 'name'."],
                            "hint": [PublicationVignettesValidator.hint],
                        }
                    ]
                }
            ],
            "Publication",
        ),
        (
            "test_data/publication_tree_bad_5.zip",
            [
                {
                    "publication_tree_bad_5/vignettes/vignette_01": [
                        {
                            "description.md": ["'figure' dict missing required element 'file'."],
                            "hint": [PublicationVignettesValidator.hint],
                        },
                        "Unexpected files in vignette: publication_tree_bad_5/vignettes/vignette_01/osmfish.json",
                    ]
                }
            ],
            "Publication",
        ),
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
        (
            "test_data/publication_tree_bad_7.zip",
            [
                {
                    "publication_tree_bad_7/vignettes/vignette_01": [
                        {
                            "description.md": [
                                "Expected data file data/codeluppi_2018_nature_methods.molecules.h5ad.zarr is absent."
                            ],
                            "hint": [PublicationVignettesValidator.hint],
                        }
                    ]
                }
            ],
            "Publication",
        ),
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
