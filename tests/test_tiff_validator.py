import zipfile
from pathlib import Path

import pytest
from test_tiff_validators_base_class import TestTiffValidators


class TestTiffValidator(TestTiffValidators):

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            ("test_data/tiff_tree_good.zip", [None], "codex"),
            (
                "test_data/tiff_tree_bad.zip",
                [
                    ".*tiff_tree_bad/dir1/notatiff.tif is not a valid TIFF file.*",
                    ".*tiff_tree_bad/dir1/dir3/notatiff.TIF is not a valid TIFF file.*",
                    ".*notatiff.TIFF is not a valid TIFF file.*",
                    ".*tiff_tree_bad/notatiff.tiff is not a valid TIFF file.*",
                ],
                "codex",
            ),
            ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
            ("test_data/codex_tree_ometiff_bad.zip", [], "codex"),
        ),
    )
    def test_tiff_validator(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        from tiff_validator import TiffValidator

        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        validator = TiffValidator(tmp_path / test_data_path.stem, assay_type)
        errors = validator.collect_errors(coreuse=4)[:]
        self.check_errors(msg_re_list, errors)
