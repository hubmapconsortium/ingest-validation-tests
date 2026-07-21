import csv
import os
import zipfile
from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pandas as pd
import pytest
from qptiff_channel_validator import (  # type: ignore
    Engine,
    QpTiffChannelComparisonValidator,
    QpTiffChannelValidator,
)


class TestQpTiffChannelValidator:

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

    @pytest.mark.parametrize(
        ("df_values", "expected_errors"),
        (
            # test case: valid float and int-like strings in both columns
            (
                {"minimum_threshold": ["0.5", "1"], "threshold": ["2.3", "-1"]},
                [],
            ),
            # test case: non-numeric value in minimum_threshold
            (
                {"minimum_threshold": ["0.5", "bad"], "threshold": ["2.3", "1"]},
                [
                    "test.qptiff.channels.csv column 'minimum_threshold' must contain only float-castable values; found 'bad'"
                ],
            ),
            # test case: non-numeric value in threshold
            (
                {"minimum_threshold": ["0.5", "1"], "threshold": ["2.3", "nope"]},
                [
                    "test.qptiff.channels.csv column 'threshold' must contain only float-castable values; found 'nope'"
                ],
            ),
            # test case: columns not present, no errors
            (
                {"other_column": ["a", "b"]},
                [],
            ),
        ),
    )
    def test_check_threshold_columns(self, df_values, expected_errors):
        df = pd.DataFrame(df_values)
        validator = QpTiffChannelValidator([Path("test")], "phenocycler")
        validator._check_threshold_columns(df, Path("test.qptiff.channels.csv"))
        assert validator.errors == expected_errors

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
            "Found 0 'qptiff.channels.csv' files in test_missing_channels_csv0/lab_processed/images directory.",
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

    ################
    # Create files #
    ################

    test_csv_filename = "test.qptiff.channels.csv"
    test_qptiff_filename = "test.qptiff"

    def _create_channels_csv_good(self, output_path: Path):
        with open(Path(output_path), "w", newline="") as mock_csv:
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

    def _create_qptiff(self, output_path: Path):
        with open(Path(output_path), "w", newline="") as mock_qptiff:
            mock_qptiff.write("good")

    ######################
    # Test files_to_test #
    ######################

    def test_exclude_raw_qptiffs(self, tmp_path):
        Path(tmp_path / "lab_processed/images").mkdir(parents=True)
        Path(tmp_path / "raw/images").mkdir(parents=True)
        self._create_channels_csv_good(
            Path(tmp_path / f"lab_processed/images/{self.test_csv_filename}")
        )
        self._create_qptiff(Path(tmp_path / f"raw/images/{self.test_qptiff_filename}"))
        with open(
            Path(tmp_path / "raw/images/test.raw.qptiff"), "w", newline=""
        ) as mock_raw_qptiff:
            mock_raw_qptiff.write("should be excluded")
        validator = QpTiffChannelValidator(
            [tmp_path],
            "phenocycler",
            schema_rows=[],
        )
        validator.files_to_test
        # raw.qptiff file exists alongside qptiff
        assert Path(tmp_path / "raw/images/test.raw.qptiff").exists()
        assert Path(tmp_path / f"raw/images/{self.test_qptiff_filename}").exists()
        # files_to_test for path has expected number of keys
        assert len(validator.files_to_test[Path(tmp_path)]) == 2
        # files_to_test for path has a value for "csv"
        assert validator.files_to_test[Path(tmp_path)]["csv"]
        # files_to_test for path includes the qptiff file and not the raw.qptiff
        assert validator.files_to_test[Path(tmp_path)]["qptiff"] == Path(
            tmp_path / f"raw/images/{self.test_qptiff_filename}"
        )

    #######################
    # Setup shared upload #
    #######################

    def _create_shared_upload_dirs(self, tmp_dir):
        Path(tmp_dir / "global/lab_processed/images").mkdir(parents=True)
        Path(tmp_dir / "global/raw/images").mkdir(parents=True)
        Path(tmp_dir / "non_global/lab_processed/images").mkdir(parents=True)
        Path(tmp_dir / "non_global/raw/images").mkdir(parents=True)

    def _create_shared_upload_validator(
        self, tmp_path: Path, non_global_file_list: dict[int, list[str]]
    ) -> QpTiffChannelValidator:
        rows = []
        for non_global_files in non_global_file_list.values():
            rows.append({"non_global_files": "; ".join(non_global_files)})
            for file in non_global_files:
                if not Path(file).exists():
                    with open(Path(tmp_path / "non_global" / file), "w", newline="") as mock_file:
                        mock_file.write("should be ignored")
        return QpTiffChannelValidator(
            [Path(tmp_path / "global"), Path(tmp_path / "non_global")],
            "phenocycler",
            schema_rows=rows,
        )

    def _set_up_shared_upload_test_dirs(
        self,
        tmp_path: Path,
        global_files: list[str] = [],
        non_global_files: dict[int, list[str]] = {},
    ):
        self._create_shared_upload_dirs(tmp_path)
        non_global_file_list = []
        for ng_file_list in non_global_files.values():
            non_global_file_list.extend(ng_file_list)
        for dir, file_list in {"global": global_files, "non_global": non_global_file_list}.items():
            if not file_list:
                continue
            for file_path in file_list:
                self.create_test_file(tmp_path, dir, file_path)

    def create_test_file(self, tmp_dir, dir, file_path):
        if file_path.endswith("csv"):
            self._create_channels_csv_good(Path(tmp_dir / dir / file_path))
        elif file_path.endswith("qptiff"):
            self._create_qptiff(Path(tmp_dir / dir / file_path))

    def get_csv_path(
        self, tmp_dir, dir: Literal["global"] | Literal["non_global"], filename: str | None = None
    ):
        if not filename:
            filename = self.test_csv_filename
        return Path(f"{tmp_dir}/{dir}/lab_processed/images/{filename}")

    def get_qptiff_path(
        self, tmp_dir, dir: Literal["global"] | Literal["non_global"], filename: str | None = None
    ):
        if not filename:
            filename = self.test_qptiff_filename
        return Path(f"{tmp_dir}/{dir}/raw/images/{filename}")

    def _set_up_shared_upload_test(
        self,
        tmp_path,
        global_files: list[str] = [],
        non_global_files: dict[int, list[str]] = {},
    ):
        self._set_up_shared_upload_test_dirs(tmp_path, global_files, non_global_files)
        return self._create_shared_upload_validator(tmp_path, non_global_files)

    ######################
    # Test shared upload #
    ######################

    def test_shared_upload_good_all_in_non_global(self, tmp_path):
        validator = self._set_up_shared_upload_test(
            tmp_path,
            non_global_files={
                0: [
                    f"raw/images/{self.test_qptiff_filename}",
                    f"lab_processed/images/{self.test_csv_filename}",
                ],
                1: [
                    f"raw/images/2nd_{self.test_qptiff_filename}",
                    f"lab_processed/images/2nd_{self.test_csv_filename}",
                ],
            },
        )
        assert validator.files_to_test == {
            0: {
                "csv": Path(
                    f"{tmp_path}/non_global/lab_processed/images/{self.test_csv_filename}"
                ),
                "qptiff": Path(f"{tmp_path}/non_global/raw/images/{self.test_qptiff_filename}"),
            },
            1: {
                "csv": Path(
                    f"{tmp_path}/non_global/lab_processed/images/2nd_{self.test_csv_filename}"
                ),
                "qptiff": Path(
                    f"{tmp_path}/non_global/raw/images/2nd_{self.test_qptiff_filename}"
                ),
            },
        }

    def test_shared_upload_good_mixed_1(self, tmp_path):
        csv_path = self.get_csv_path(tmp_path, "global")
        validator = self._set_up_shared_upload_test(
            tmp_path,
            global_files=[str(csv_path)],
            non_global_files={
                0: [f"raw/images/{self.test_qptiff_filename}"],
                1: [f"raw/images/2nd_{self.test_qptiff_filename}"],
            },
        )
        assert validator.files_to_test == {
            0: {
                "csv": csv_path,
                "qptiff": self.get_qptiff_path(tmp_path, "non_global"),
            },
            1: {
                "csv": csv_path,
                "qptiff": self.get_qptiff_path(
                    tmp_path, "non_global", f"2nd_{self.test_qptiff_filename}"
                ),
            },
        }

    def test_shared_upload_good_mixed_2(self, tmp_path):
        validator = self._set_up_shared_upload_test(
            tmp_path,
            global_files=[str(self.get_qptiff_path(tmp_path, "global"))],
            non_global_files={
                0: [f"lab_processed/images/{self.test_csv_filename}"],
                1: [f"lab_processed/images/2nd_{self.test_csv_filename}"],
            },
        )
        shared_qptiff_path = self.get_qptiff_path(tmp_path, "global", self.test_qptiff_filename)
        assert validator.files_to_test == {
            0: {
                "csv": Path(self.get_csv_path(tmp_path, "non_global")),
                "qptiff": shared_qptiff_path,
            },
            1: {
                "csv": self.get_csv_path(tmp_path, "non_global", f"2nd_{self.test_csv_filename}"),
                "qptiff": shared_qptiff_path,
            },
        }

    def test_shared_upload_good_all_in_global(self, tmp_path):
        """
        Only want one key in files_to_test if both files are in global to prevent
        multiple identical loops.
        """
        csv_file = self.get_csv_path(tmp_path, "global")
        qptiff_file = self.get_qptiff_path(tmp_path, "global")
        validator = self._set_up_shared_upload_test(
            tmp_path,
            global_files=[str(csv_file), str(qptiff_file)],
            non_global_files={0: ["file1.txt", "file2.ome.tiff"], 1: ["file2.txt"]},
        )
        assert validator.files_to_test == {
            0: {
                "csv": csv_file,
                "qptiff": qptiff_file,
            }
        }

    def test_shared_upload_bad_missing_file(self, tmp_path):
        """
        Detect missing file between non_global and global dirs
        """
        csv_file = self.get_csv_path(tmp_path, "global")
        qptiff_file = self.get_qptiff_path(tmp_path, "global")
        validator = self._set_up_shared_upload_test(
            tmp_path,
            global_files=[str(csv_file), str(qptiff_file)],
            non_global_files={0: ["file1.txt"], 1: ["file2.txt"]},
        )
        os.remove(Path(tmp_path / f"global/lab_processed/images/{self.test_csv_filename}"))
        validator.files_to_test
        assert validator.errors == [
            "Unable to find csv file for dataset row 0 in shared upload.",
            "Unable to find csv file for dataset row 1 in shared upload.",
        ]

    def test_shared_upload_bad_file_missing_in_tsv(self, tmp_path):
        """
        File in non_global missing from metadata.tsv > non_global_files
        """
        self._set_up_shared_upload_test(
            tmp_path,
            non_global_files={
                0: [
                    f"raw/images/{self.test_qptiff_filename}",
                    f"lab_processed/images/{self.test_csv_filename}",
                ],
                1: [
                    f"raw/images/2nd_{self.test_qptiff_filename}",
                    f"lab_processed/images/2nd_{self.test_csv_filename}",
                ],
            },
        )
        validator = QpTiffChannelValidator(
            [Path(tmp_path / "global"), Path(tmp_path / "non_global")],
            "phenocycler",
            schema_rows=[
                {
                    "non_global_files": f"./lab_processed/images/{self.test_csv_filename}",
                },
                {
                    "non_global_files": f"./lab_processed/images/2nd_{self.test_csv_filename}",
                },
            ],
        )
        validator.files_to_test
        assert validator.errors == [
            "Unable to find qptiff file for dataset row 0 in shared upload.",
            "Unable to find qptiff file for dataset row 1 in shared upload.",
        ]

    def test_shared_upload_bad_multiple_global(self, tmp_path):
        """
        Multiple channels.csv files found in global
        """
        csv_file = self.get_csv_path(tmp_path, "global")
        qptiff_file = self.get_qptiff_path(tmp_path, "global")
        validator = self._set_up_shared_upload_test(
            tmp_path,
            global_files=[str(csv_file), str(qptiff_file)],
            non_global_files={0: ["file1.txt"], 1: ["file2.txt"]},
        )
        self._create_channels_csv_good(
            Path(tmp_path / f"global/lab_processed/images/2nd_{self.test_csv_filename}"),
        )
        validator.files_to_test
        assert validator.errors == [
            "Found 2 global csvs (global/lab_processed/images/2nd_test.qptiff.channels.csv, global/lab_processed/images/test.qptiff.channels.csv)."
        ]

    def test_shared_upload_mixed_outcome(self, monkeypatch, tmp_path):
        """
        One file pair is fine, the other is missing a file.
        Make sure the good pair is validated and the error is logged for the bad pair.
        """
        validator = self._set_up_shared_upload_test(
            tmp_path,
            non_global_files={
                0: [
                    f"raw/images/{self.test_qptiff_filename}",
                    f"lab_processed/images/{self.test_csv_filename}",
                ],
                1: [
                    f"lab_processed/images/2nd_{self.test_csv_filename}",
                ],
            },
        )
        mock = Mock()
        monkeypatch.setattr(QpTiffChannelValidator, "check_qptiff_channels_file", mock)
        validator.files_to_test
        assert validator.errors == [
            "Unable to find qptiff file for dataset row 1 in shared upload."
        ]
        csv_path = Path(tmp_path / f"non_global/lab_processed/images/{self.test_csv_filename}")
        assert len(validator.files_to_test) == 1
        assert validator.files_to_test[0]["csv"] == csv_path
        validator.collect_errors()
        mock.assert_called_with(csv_path)


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
