import os
import zipfile
from pathlib import Path

import pytest
from qptiff_channel_validator import QpTiffChannelValidator


class TestQpTiffChannelCsv:
    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            # test case: both columns missing Yes/true value
            (
                "test_data/qptiff_both_missing.zip",
                [
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_nuclei_segmentation'",
                ],
                "phenocycler",
            ),
            # test case: one column missing Yes/true value
            (
                "test_data/qptiff_one_missing.zip",
                [
                    "qptiff_one_missing/lab_processed/images/qptiff_one_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'"
                ],
                "phenocycler",
            ),
            # test case: both columns have Yes/true values
            ("test_data/qptiff_good.zip", [], "phenocycler"),
            # test case: both columns have Yes/true values, column names have spaces
            (
                "test_data/qptiff_good_with_alt_column_format.zip",
                [],
                "phenocycler",
            ),
            # test case: columns out of order
            (
                "test_data/qptiff_bad_column_order.zip",
                [
                    "qptiff_bad_column_order/lab_processed/images/qptiff_bad_column_order.qptiff.channels.csv: 'is_antibody' must be column 4",
                    "qptiff_bad_column_order/lab_processed/images/qptiff_bad_column_order.qptiff.channels.csv: 'is_channel_used_for_cell_segmentation' must be column 3",
                    "qptiff_bad_column_order/lab_processed/images/qptiff_bad_column_order.qptiff.channels.csv: 'is_channel_used_for_nuclei_segmentation' must be column 2",
                ],
                "phenocycler",
            ),
            # test case: extra column interrupts required column order
            (
                "test_data/qptiff_bad_extra_column.zip",
                [
                    "Unexpected column header found in column 3: 'bad'. Columns 1-4 must match required order. Can't validate qptiff_bad_extra_column/lab_processed/images/qptiff_bad_extra_column.qptiff.channels.csv."
                ],
                "phenocycler",
            ),
            # test case: wrong assay type
            ("test_data/qptiff_good.zip", [], "snRNAseq"),
        ),
    )
    def test_qptiff_channel_csv(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        validator = QpTiffChannelValidator(tmp_path / test_data_path.stem, assay_type)
        validator.check_qptiff_channels_file(
            Path(
                tmp_path
                / test_data_path.stem
                / f"lab_processed/images/{test_data_path.stem}.qptiff.channels.csv"
            )
        )
        for error in msg_re_list:
            assert error in validator.errors

    def test_missing_required_dir(self, tmp_path):
        validator = QpTiffChannelValidator(tmp_path, "phenocycler")
        errors = validator.collect_errors()[:]
        errors.sort()
        for err in [
            "Can't find 'lab_processed/images' subdirectory in 'test_missing_required_dir0'.",
            "Can't find 'raw/images' subdirectory in 'test_missing_required_dir0'.",
            "Could not find qptiff.channels.csv and associated QPTIFF files (required for phenocycler).",
        ]:
            assert err in errors

    def test_missing_channels_csv(self, tmp_path):
        dir1 = tmp_path / "lab_processed"
        dir1.mkdir()
        dir2 = dir1 / "images"
        dir2.mkdir()
        validator = QpTiffChannelValidator(tmp_path, "phenocycler")
        errors = validator.collect_errors()[:]
        errors.sort()
        assert (
            "Could not find qptiff.channels.csv and associated QPTIFF files (required for phenocycler)."
            in errors
        )

    @pytest.mark.parametrize(
        ("test_data_fnames", "msg_re_list"),
        (
            (
                [
                    Path("test_data/qptiff_both_missing.zip"),
                    Path("test_data/qptiff_one_missing.zip"),
                ],
                [
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_nuclei_segmentation'",
                    "qptiff_one_missing/lab_processed/images/qptiff_one_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
                ],
            ),
            (
                [
                    Path("test_data/qptiff_bad_extra_column.zip"),
                    Path("test_data/qptiff_both_missing.zip"),
                ],
                [
                    "Unexpected column header found in column 3: 'bad'. Columns 1-4 must match required order. Can't validate qptiff_bad_extra_column/lab_processed/images/qptiff_bad_extra_column.qptiff.channels.csv.",
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
                    "qptiff_both_missing/lab_processed/images/qptiff_both_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_nuclei_segmentation'",
                ],
            ),
            (
                [
                    Path("test_data/qptiff_good.zip"),
                    Path("test_data/qptiff_one_missing.zip"),
                ],
                [
                    "qptiff_one_missing/lab_processed/images/qptiff_one_missing.qptiff.channels.csv must have at least one 'Yes' value in column 'is_channel_used_for_cell_segmentation'",
                ],
            ),
        ),
    )
    def test_multiple_files(self, test_data_fnames, msg_re_list, tmp_path):
        for test_data_fname in test_data_fnames:
            test_data_path = Path(test_data_fname)
            zfile = zipfile.ZipFile(test_data_path)
            zfile.extractall(tmp_path)
        test_data_paths = [tmp_path / test_data_path.stem for test_data_path in test_data_fnames]
        validator = QpTiffChannelValidator(test_data_paths, "phenocycler")
        for data_path in test_data_paths:
            validator.check_qptiff_channels_file(
                data_path / f"lab_processed/images/{data_path.stem}.qptiff.channels.csv"
            )
        for error in msg_re_list:
            assert error in validator.errors

    def make_shared_upload(self, parent_dir, lab_processed_files: list[str], raw_files: list[str]):
        os.makedirs(parent_dir / "non_global/lab_processed/images")
        os.makedirs(parent_dir / "non_global/raw/images")
        for file in lab_processed_files:
            os.mknod(f"{parent_dir}/non_global/lab_processed/images/{file}")
        for file in raw_files:
            os.mknod(f"{parent_dir}/non_global/raw/images/{file}")

    # TODO: need to patch in schema.rows
    def test_shared_upload_good(self, tmp_path):
        self.make_shared_upload(tmp_path, ["test.qptiff.channels.csv"], ["test.qptiff"])
        validator = QpTiffChannelValidator(tmp_path / "non_global", "phenocycler")
        assert validator.get_file_pairs_to_test() == {
            "non_global/lab_processed/images/test.qptiff.channels.csv": "non_global/raw/images/test.qptiff"
        }
