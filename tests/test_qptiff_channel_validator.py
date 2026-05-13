import csv
import os
import zipfile
from pathlib import Path
from unittest.mock import Mock

import pytest
from qptiff_channel_validator import (  # type: ignore
    Engine,
    QpTiffChannelComparisonValidator,
    QpTiffChannelValidator,
)


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
            "Did not find expected directory test_missing_required_dir0/lab_processed/images",
            "Did not find expected directory test_missing_required_dir0/raw/images",
        ]:
            assert err in errors

    def test_missing_channels_csv(self, tmp_path):
        Path(tmp_path / "lab_processed/images").mkdir(parents=True)
        Path(tmp_path / "raw/images").mkdir(parents=True)
        validator = QpTiffChannelValidator(tmp_path, "phenocycler")
        errors = validator.collect_errors()[:]
        errors.sort()
        for err in [
            "Found 0 .channels.csv files in test_missing_channels_csv0/lab_processed/images directory.",
        ]:
            assert err in errors

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

    #######################
    # Setup shared upload #
    #######################

    test_csv_filename = "test.qptiff.channels.csv"
    test_qptiff_filename = "test.qptiff"

    def _create_shared_upload_dirs(self, tmp_dir):
        Path(tmp_dir / "global/lab_processed/images").mkdir(parents=True)
        Path(tmp_dir / "global/raw/images").mkdir(parents=True)
        Path(tmp_dir / "non_global/lab_processed/images").mkdir(parents=True)
        Path(tmp_dir / "non_global/raw/images").mkdir(parents=True)

    def _create_channels_csv_good(self, output_dir: Path, filename: str | None = None):
        if not filename:
            filename = self.test_csv_filename
        with open(Path(output_dir / filename), "w", newline="") as mock_csv:
            writer = csv.writer(mock_csv)
            writer.writerows(
                [
                    {
                        "channel_id": "Channel:0:0",
                        "is_channel_used_for_nuclei_segmentation": "Yes",
                        "is_channel_used_for_cell_segmentation": "No",
                        "is_antibody": "No",
                    }
                ]
            )

    def _create_qptiff(self, output_dir: Path):
        with open(Path(output_dir / self.test_qptiff_filename), "w", newline="") as mock_qptiff:
            mock_qptiff.write("good")

    def _create_shared_upload_validator(
        self, tmp_path: Path, data_path_to_non_global: dict[str, str]
    ) -> QpTiffChannelValidator:
        rows = []
        for data_path, non_global_files in data_path_to_non_global.items():
            rows.append({"data_path": data_path, "non_global_files": non_global_files})
        return QpTiffChannelValidator(
            [Path(tmp_path / "global"), Path(tmp_path / "non_global")],
            "phenocycler",
            schema_rows=rows,
        )

    def _set_up_shared_upload_test(self, tmp_path, csv_in_global: bool, qptiff_in_global: bool):
        self._create_shared_upload_dirs(tmp_path)
        csv_path = (
            "global/lab_processed/images" if csv_in_global else "non_global/lab_processed/images"
        )
        qptiff_path = "global/raw/images" if qptiff_in_global else "non_global/raw/images"
        self._create_channels_csv_good(Path(tmp_path / csv_path))
        self._create_qptiff(Path(tmp_path / qptiff_path))
        non_global_files = []
        if not csv_in_global:
            non_global_files.append(f"./lab_processed/images/{self.test_csv_filename}")
        if not qptiff_in_global:
            non_global_files.append(f"./raw/images/{self.test_qptiff_filename}")
        return self._create_shared_upload_validator(
            tmp_path, {"./data_path_1": "; ".join(non_global_files)}
        )

    ######################
    # Test shared upload #
    ######################

    def test_shared_upload_good_all_in_non_global(self, tmp_path):
        validator = self._set_up_shared_upload_test(
            tmp_path, csv_in_global=False, qptiff_in_global=False
        )
        assert validator.files_to_test == {
            "./data_path_1": {
                "csv": Path(
                    f"{tmp_path}/non_global/lab_processed/images/{self.test_csv_filename}"
                ),
                "qptiff": Path(f"{tmp_path}/non_global/raw/images/{self.test_qptiff_filename}"),
            }
        }

    def test_shared_upload_good_mixed1(self, tmp_path):
        validator = self._set_up_shared_upload_test(
            tmp_path, csv_in_global=True, qptiff_in_global=False
        )
        assert validator.files_to_test == {
            "./data_path_1": {
                "csv": Path(f"{tmp_path}/global/lab_processed/images/{self.test_csv_filename}"),
                "qptiff": Path(f"{tmp_path}/non_global/raw/images/{self.test_qptiff_filename}"),
            }
        }

    def test_shared_upload_good_mixed_2(self, tmp_path):
        validator = self._set_up_shared_upload_test(
            tmp_path, csv_in_global=False, qptiff_in_global=True
        )
        assert validator.files_to_test == {
            "./data_path_1": {
                "csv": Path(
                    f"{tmp_path}/non_global/lab_processed/images/{self.test_csv_filename}"
                ),
                "qptiff": Path(f"{tmp_path}/global/raw/images/{self.test_qptiff_filename}"),
            }
        }

    def test_shared_upload_good_all_in_global(self, tmp_path):
        validator = self._set_up_shared_upload_test(
            tmp_path, csv_in_global=True, qptiff_in_global=True
        )
        assert validator.files_to_test == {
            "./data_path_1": {
                "csv": Path(f"{tmp_path}/global/lab_processed/images/{self.test_csv_filename}"),
                "qptiff": Path(f"{tmp_path}/global/raw/images/{self.test_qptiff_filename}"),
            }
        }

    def test_shared_upload_bad_missing_file(self, tmp_path):
        """
        Detect missing file between non_global and global dirs
        """
        validator = self._set_up_shared_upload_test(
            tmp_path, csv_in_global=True, qptiff_in_global=True
        )
        os.remove(Path(tmp_path / f"global/lab_processed/images/{self.test_csv_filename}"))
        validator.files_to_test
        assert validator.errors == [
            "Found 1 qptiffs and 0 channels.csv paths for dataset ./data_path_1 in shared upload."
        ]

    def test_shared_upload_bad_file_missing_in_tsv(self, tmp_path):
        """
        File in non_global missing from metadata.tsv > non_global_files
        """
        self._set_up_shared_upload_test(tmp_path, csv_in_global=False, qptiff_in_global=False)
        validator = QpTiffChannelValidator(
            [Path(tmp_path / "global"), Path(tmp_path / "non_global")],
            "phenocycler",
            schema_rows=[
                {
                    "data_path": "./data_path_1",
                    "non_global_files": f"./lab_processed/images/{self.test_csv_filename}",
                }
            ],
        )
        validator.files_to_test
        assert validator.errors == [
            "File(s) non_global/raw/images/test.qptiff found but missing from non_global_files column in metadata.tsv."
        ]

    def test_shared_upload_bad_extra_file_in_tsv(self, tmp_path):
        """
        File in metadata.tsv > non_global_files is actually in global
        """
        self._set_up_shared_upload_test(tmp_path, csv_in_global=True, qptiff_in_global=True)
        validator = QpTiffChannelValidator(
            [Path(tmp_path / "global"), Path(tmp_path / "non_global")],
            "phenocycler",
            schema_rows=[
                {
                    "data_path": "./data_path_1",
                    "non_global_files": f"./lab_processed/images/{self.test_csv_filename}",
                }
            ],
        )
        validator.files_to_test
        assert validator.errors == [
            "Path non_global/lab_processed/images/test.qptiff.channels.csv doesn't exist."
        ]

    def test_shared_upload_bad_multiple_global(self, tmp_path):
        """
        Multiple channels.csv files found in global
        """
        self._set_up_shared_upload_test(tmp_path, csv_in_global=True, qptiff_in_global=True)
        self._create_channels_csv_good(
            Path(tmp_path / "global/lab_processed/images/"),
            filename=f"2nd_{self.test_csv_filename}",
        )
        validator = QpTiffChannelValidator(
            [Path(tmp_path / "global"), Path(tmp_path / "non_global")],
            "phenocycler",
            schema_rows=[
                {
                    "data_path": "./data_path_1",
                    "non_global_files": f"/something/else",
                }
            ],
        )
        validator.files_to_test
        assert validator.errors == [
            "Found 1 qptiffs and 2 channels.csv paths for dataset ./data_path_1 in shared upload."
        ]


class TestQptiffChannelComparisonValidator:
    @pytest.fixture(autouse=True)
    def _mock_validator_good(self, monkeypatch):
        monkeypatch.setattr(QpTiffChannelComparisonValidator, "uuid", "test_uuid")

    def test_check_tmp_dir(self, monkeypatch, tmp_path):
        v = QpTiffChannelComparisonValidator(tmp_path, "phenocycler")
        monkeypatch.setattr(QpTiffChannelComparisonValidator, "tmp_dir_base", tmp_path)
        tmp_dir = Path(v.tmp_dir_base / "test_uuid_ome_xml")
        assert not tmp_dir.exists()
        v._check_tmp_dir()
        assert tmp_dir.exists()
        v._check_tmp_dir()  # should pass

    def test_engine_get_ome_xml_channels(self):
        e = Engine()
        channels = e.get_ome_xml_channels("test_data/minimal.ome.xml")
        assert channels == {"Channel:0:0"}

    def test_engine_get_csv_channels(self):
        e = Engine()
        channels = e.get_csv_channels("test_data/qptiff_good.qptiff.channels.csv")
        assert channels == {"Channel:0:0", "Channel:0:1", "Channel:0:2", "Channel:0:3"}

    def test_engine_compare_channels_good(self, monkeypatch):
        mock_qptiff_channels = Mock(
            return_value={"Channel:0:0", "Channel:0:1", "Channel:0:2", "Channel:0:3"}
        )
        monkeypatch.setattr(Engine, "get_qptiff_channels", mock_qptiff_channels)
        e = Engine()
        assert (
            e(
                Path("data_path"),
                {
                    "csv": Path("test_data/qptiff_good.qptiff.channels.csv"),
                    "qptiff": Path("qptiff_path"),
                },
                Path("tmp_dir_path"),
            )
            == None
        )

    def test_engine_compare_channels_bad(self, monkeypatch):
        mock_qptiff_channels = Mock(return_value={"Channel:0:0", "Channel:0:1", "Channel:0:2"})
        monkeypatch.setattr(Engine, "get_qptiff_channels", mock_qptiff_channels)
        e = Engine()
        errors = e(
            Path("data_path"),
            {
                "csv": Path("test_data/qptiff_good.qptiff.channels.csv"),
                "qptiff": Path("qptiff_path"),
            },
            Path("tmp_dir_path"),
        ).splitlines()
        cleaned_err = [err.strip() for err in errors if err != ""]
        expected_err = [
            "Channels in test_data/qptiff_good.qptiff.channels.csv don't match those in QPTIFF qptiff_path (from converted OME-XML).",
            "Channels in CSV not present in QPTIFF: Channel:0:3",
        ]
        for err in expected_err:
            assert err in cleaned_err
