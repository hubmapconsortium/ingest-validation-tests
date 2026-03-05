import zipfile
from pathlib import Path

import pytest
from qptiff_channel_validator import QpTiffChannelValidator


class TestQpTiffChannelValidator:
    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            # test case: both columns missing Yes value
            (
                "test_data/qptiff_both_missing.zip",
                [
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_nuclei_segmentation'",
                ],
                "phenocycler",
            ),
            # test case: one column missing Yes value
            (
                "test_data/qptiff_one_missing.zip",
                [
                    "qptiff_one_missing/lab_processed/images/qptiff_one_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'"
                ],
                "phenocycler",
            ),
            # test case: both columns have Yes value
            ("test_data/qptiff_good.zip", [None], "phenocycler"),
            # test case: both columns have Yes value, column names have spaces
            (
                "test_data/qptiff_good_with_alt_column_format.zip",
                [None],
                "phenocycler",
            ),
            # test case: wrong assay type
            ("test_data/qptiff_good.zip", [], "snRNAseq"),
        ),
    )
    def test_qptiff_channel_validator(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        validator = QpTiffChannelValidator(tmp_path / test_data_path.stem, assay_type)
        errors = validator.collect_errors()[:]
        errors.sort()
        assert errors == msg_re_list

    def test_missing_required_dir(self, tmp_path):
        validator = QpTiffChannelValidator(tmp_path, "phenocycler")
        errors = validator.collect_errors()[:]
        errors.sort()
        assert errors == [
            "Can't find 'lab_processed/images' subdirectory in 'test_missing_required_dir0'.",
            "Could not find 'lab_processed/images/*.qptiff.channels.csv' files (required for phenocycler).",
        ]

    def test_missing_required_files(self, tmp_path):
        dir1 = tmp_path / "lab_processed"
        dir1.mkdir()
        dir2 = dir1 / "images"
        dir2.mkdir()
        validator = QpTiffChannelValidator(tmp_path, "phenocycler")
        errors = validator.collect_errors()[:]
        errors.sort()
        assert errors == [
            "Could not find 'lab_processed/images/*.qptiff.channels.csv' files (required for phenocycler).",
        ]

    def test_multiple_files_with_errors(self, tmp_path):
        test_data = [
            Path("test_data/qptiff_both_missing.zip"),
            Path("test_data/qptiff_one_missing.zip"),
        ]
        for test_data_fname in test_data:
            test_data_path = Path(test_data_fname)
            zfile = zipfile.ZipFile(test_data_path)
            zfile.extractall(tmp_path)
        validator = QpTiffChannelValidator(
            [tmp_path / test_data_path.stem for test_data_path in test_data], "phenocycler"
        )
        errors = validator.collect_errors()[:]
        errors.sort()
        assert errors == [
            "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
            "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_nuclei_segmentation'",
            "qptiff_one_missing/lab_processed/images/qptiff_one_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
        ]

    def test_multiple_files_mixed_result(self, tmp_path):
        test_data = [
            Path("test_data/qptiff_good.zip"),
            Path("test_data/qptiff_one_missing.zip"),
        ]
        for test_data_fname in test_data:
            test_data_path = Path(test_data_fname)
            zfile = zipfile.ZipFile(test_data_path)
            zfile.extractall(tmp_path)
        validator = QpTiffChannelValidator(
            [tmp_path / test_data_path.stem for test_data_path in test_data], "phenocycler"
        )
        errors = validator.collect_errors()[:]
        errors.sort()
        assert errors == [
            "qptiff_one_missing/lab_processed/images/qptiff_one_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
        ]
