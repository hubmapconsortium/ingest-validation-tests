import zipfile
from pathlib import Path, PurePosixPath
from unittest.mock import MagicMock, patch

import pytest
from test_tiff_validators_base_class import TestTiffValidators

from src.ingest_validation_tests.geojson_tiff_validator import GeoJsonTiffValidator


class TestGeoJsonTiffValidator(TestTiffValidators):

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            # GeoJSON intersects TIFF bounds → no errors
            ("test_data/geojson_tiff_good.zip", [None], "visium"),
            # GeoJSON outside TIFF bounds → error
            (
                "test_data/geojson_tiff_bad.zip",
                [
                    r".*tissue_boundary\.geojson: GeoJSON boundaries do not intersect any TIFF pixel dimensions.*",
                ],
                "visium",
            ),
            # No GeoJSON files → validator skips, returns []
            ("test_data/geojson_tiff_no_geojson.zip", [], "visium"),
            # GeoJSON + >1 OME-TIFF → error
            (
                "test_data/geojson_count_multi_ome.zip",
                [r"GeoJSON present but found 2 OME-TIFFs \(expected 1\):.*"],
                "visium",
            ),
            # GeoJSON + 0 OME-TIFF + >1 QPTIFF → error
            (
                "test_data/geojson_count_multi_qptiff.zip",
                [r"GeoJSON present but found 2 QPTIFFs and no OME-TIFFs \(expected 1 QPTIFF\):.*"],
                "visium",
            ),
            # GeoJSON + 1 OME-TIFF → no error
            ("test_data/geojson_count_one_ome.zip", [None], "visium"),
            # GeoJSON + 1 QPTIFF, no OME-TIFF → no error
            ("test_data/geojson_count_one_qptiff.zip", [None], "visium"),
            # OME-TIFF in extras/ excluded from count → no error
            ("test_data/geojson_count_extras_excluded.zip", [None], "visium"),
            # GeoJSON intersects QPTIFF pixel bounds → no error
            ("test_data/geojson_qptiff_good.zip", [None], "visium"),
            # GeoJSON outside QPTIFF pixel bounds → error
            (
                "test_data/geojson_qptiff_bad.zip",
                [
                    r".*tissue_boundary\.geojson: GeoJSON boundaries do not intersect any TIFF pixel dimensions.*",
                ],
                "visium",
            ),
        ),
    )
    def test_geojson_tiff_validator(self, test_data_fname, msg_re_list, assay_type, tmp_path):

        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        validator = GeoJsonTiffValidator(tmp_path / test_data_path.stem, assay_type)
        errors = validator.collect_errors()[:]
        self.check_errors(msg_re_list, errors)

    def test_get_file_path(self, tmp_path):
        good_filenames = [
            PurePosixPath(tmp_path / "test.qptiff"),
            PurePosixPath(tmp_path / "test_dir/test.qptiff"),
            PurePosixPath(tmp_path / "test_dir/extras.qptiff"),
        ]
        bad_filenames = [
            PurePosixPath(tmp_path / "test.qptiff.raw"),
            PurePosixPath(tmp_path / "test.raw.qptiff"),
            PurePosixPath(tmp_path / "test_dir/test.qptiff.raw"),
            PurePosixPath(tmp_path / "test_dir/test.raw.qptiff"),
            PurePosixPath(tmp_path / "test_dir/extras/test.qptiff"),
            PurePosixPath(tmp_path / "extras/test_dir/test.qptiff"),
        ]
        Path(tmp_path / "test_dir/extras").mkdir(parents=True)
        Path(tmp_path / "extras/test_dir").mkdir(parents=True)
        for file in [*good_filenames, *bad_filenames]:
            with open(file, "w", newline="") as mock_file:
                mock_file.write("should be ignored")
        validator = GeoJsonTiffValidator([tmp_path], "")
        assert validator._get_qptiffs_for_data_path(tmp_path) == good_filenames

