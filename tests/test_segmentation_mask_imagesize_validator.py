import re
import zipfile
from pathlib import Path
from dataclasses import dataclass, field
from unittest.mock import patch

import pandas as pd  # to parse metadata.tsv

import pytest

import tests_utils


@dataclass
class MySchemaVersion:
    def __init__(self, rows):
        self.rows = rows


class MyGetParentData:
    def __init__(self, parent_dataset_id, token, app_ctx):
        self.parent_dataset_id = parent_dataset_id
        self.token = token
        self.app_ctx = app_ctx
    def get_path(self):
        print(f"IN GET PATH {self.parent_dataset_id} {self.token} {self.app_ctx}")
        return "foo/bar/baz"


@pytest.mark.parametrize(
    ("test_data_fname", "msg_re_list", "assay_type"),
    (
        # (
        #     "test_data/codex_tree_ometiff_bad.zip",
        #     [".*tubhiswt_C0_bad.ome.tif is not a valid OME.TIFF file.*"],
        #     "CODEX",
        # ),
        # ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
        ("test_data/seg_mask_goodtiff.zip", [None], "segmentation mask"),
    ),
)
def test_segmentation_mask_imagesize_validator(
        test_data_fname, msg_re_list, assay_type, tmp_path
):
    test_data_path = Path(test_data_fname)
    zfile = zipfile.ZipFile(test_data_path)
    zfile.extractall(tmp_path)
    with patch("tests_utils.GetParentData", MyGetParentData) as mock_class:
        from segmentation_mask_imagesize_validation import ImageSizeValidator

        tsv_path_l = list((tmp_path / test_data_path.stem).glob("*_metadata.tsv"))
        assert len(tsv_path_l) == 1, "Failed to find one metadata file"
        recs_df = pd.read_csv(tsv_path_l[0], sep="\t")
        sv = MySchemaVersion(recs_df.to_dict("records"))
        validator = ImageSizeValidator(tmp_path / test_data_path.stem, assay_type,
                                       metadata_tsv=sv)
        errors = validator.collect_errors(coreuse=4)[:]
    assert len(msg_re_list) == len(errors)
    for err_str, re_str in zip(errors, msg_re_list):
        assert (err_str is None and re_str is None) or re.match(
            re_str, err_str, flags=re.MULTILINE
        )
