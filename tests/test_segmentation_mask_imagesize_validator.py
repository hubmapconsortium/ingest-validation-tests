import re
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd  # to parse metadata.tsv
import pytest

CROP512_SIZE = {
    "X": 0.2520000043344001,
    "XUnits": "µm",
    "XPix": 512,
    "Y": 0.2520000043344001,
    "YUnits": "µm",
    "YPix": 512,
    "Z": None,
    "ZUnits": "µm",
    "ZPix": 1,
}
DIFFERENT_SIZE = {**CROP512_SIZE, "XPix": 400, "YPix": 400}


@pytest.mark.parametrize(
    ("test_data_fname", "parent_data_fname", "msg_re_list", "assay_type"),
    (
        (
            "test_data/seg_mask_goodtiff.zip",
            "test_data/good_codex_akoya_directory_v1_with_dataset_json_passes.zip",
            ["Too many or too few files Base Images"],
            "segmentation mask",
        ),
        (
            "test_data/segmask_HBM787.DVDV.435_crop512.zip",
            "test_data/pas_HBM847.ZQZH.768_crop512.zip",
            [None],
            "segmentation mask",
        ),
        (
            "test_data/segmask_HBM787.DVDV.435_crop400.zip",
            "test_data/pas_HBM847.ZQZH.768_crop512.zip",
            ["No segmentation mask image matches any base image size"],
            "segmentation mask",
        ),
        (
            "test_data/segmask_HBM787.DVDV.435_crop400.zip",
            "test_data/pas_HBM847.ZQZH.768_crop512.zip",
            [],
            "bad_type",
        ),
    ),
)
@patch("tests_utils.GetParentData.get_path")
def test_segmentation_mask_imagesize_validator(
    mock_method, test_data_fname, parent_data_fname, msg_re_list, assay_type, tmp_path
):
    test_data_path = Path(test_data_fname)
    seg_zfile = zipfile.ZipFile(test_data_path)
    parent_data_path = Path(parent_data_fname)
    parent_zfile = zipfile.ZipFile(parent_data_path)
    tmp_seg_path = tmp_path / "segfile"
    tmp_parent_path = tmp_path / "parent"
    tmp_seg_path.mkdir(parents=True, exist_ok=True)
    tmp_parent_path.mkdir(parents=True, exist_ok=True)
    seg_zfile.extractall(tmp_seg_path)
    parent_zfile.extractall(tmp_parent_path)
    mock_method.return_value = tmp_parent_path

    from segmentation_mask_imagesize_validation import ImageSizeValidator

    tsv_path_l = list((tmp_seg_path / test_data_path.stem).glob("*metadata.tsv"))
    assert len(tsv_path_l) == 1, "Failed to find one metadata file"
    recs_df = pd.read_csv(tsv_path_l[0], sep="\t")
    validator = ImageSizeValidator(
        tmp_seg_path / test_data_path.stem,
        assay_type,
        schema_rows=recs_df.to_dict("records"),
        coreuse=4,
    )
    errors = validator.collect_errors()[:]
    assert len(msg_re_list) == len(errors)
    for err_str, re_str in zip(errors, msg_re_list):
        assert (err_str is None and re_str is None) or re.match(
            re_str, err_str, flags=re.MULTILINE
        )


@patch("tests_utils.GetParentData.get_path")
@patch("segmentation_mask_imagesize_validation.get_ometiff_sizes")
def test_multi_image_parent_match_passes(mock_get_sizes, mock_get_path, tmp_path):
    """Parent OME-TIFF with multiple images: passes when any image matches mask."""
    test_data_path = Path("test_data/segmask_HBM787.DVDV.435_crop512.zip")
    parent_data_path = Path("test_data/pas_HBM847.ZQZH.768_crop512.zip")
    tmp_seg_path = tmp_path / "segfile"
    tmp_parent_path = tmp_path / "parent"
    tmp_seg_path.mkdir(parents=True, exist_ok=True)
    tmp_parent_path.mkdir(parents=True, exist_ok=True)
    zipfile.ZipFile(test_data_path).extractall(tmp_seg_path)
    zipfile.ZipFile(parent_data_path).extractall(tmp_parent_path)
    mock_get_path.return_value = tmp_parent_path

    mock_get_sizes.side_effect = lambda f: (
        [CROP512_SIZE]
        if "segmentation_masks" in str(f)
        else [DIFFERENT_SIZE, CROP512_SIZE]
    )

    from segmentation_mask_imagesize_validation import ImageSizeValidator

    tsv_path_l = list((tmp_seg_path / test_data_path.stem).glob("*metadata.tsv"))
    assert len(tsv_path_l) == 1
    recs_df = pd.read_csv(tsv_path_l[0], sep="\t")
    validator = ImageSizeValidator(
        tmp_seg_path / test_data_path.stem,
        "segmentation mask",
        schema_rows=recs_df.to_dict("records"),
        coreuse=4,
    )
    errors = validator.collect_errors()[:]
    assert errors == [None]
