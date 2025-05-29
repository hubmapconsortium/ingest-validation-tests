import zipfile
from pathlib import Path

import pytest
from test_tiff_validators_base_class import TestTiffValidators


class TestOmeTiffFieldValidator(TestTiffValidators):

    def validator(self, test_data_fname, assay_type, tmp_path):
        from ome_tiff_field_validator import OmeTiffFieldValidator

        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        return OmeTiffFieldValidator(tmp_path / test_data_path.stem, assay_type)

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            (
                "test_data/codex_tree_ometiff_bad.zip",
                [
                    ".*/codex_tree_ometiff_bad/tubhiswt_C0_bad.ome.tif is not a valid OME.TIFF file: Failed to read OME XML",
                    ".*/codex_tree_ometiff_bad/sample1.ome.tif is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    ".*/codex_tree_ometiff_bad/sample2.ome.tif is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                ],
                "CODEX",
            ),
            ("test_data/codex_tree_ometiff_good.zip", [None], "CODEX"),
            ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                ],
                "PAS",
            ),
        ),
    )
    def test_ome_tiff_field_validator(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        validator = self.validator(test_data_fname, assay_type, tmp_path)
        errors = validator.collect_errors(coreuse=4)[:]
        self.check_errors(msg_re_list, errors)

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            ("test_data/fake_snrnaseq_tree_good.zip", [], "test_dataset_type"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'test_ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'; missing required attribute 'PhysicalSizeZ'",
                ],
                "test_dataset_type",
            ),
            ("test_data/codex_tree_ometiff_good.zip", [None], "CODEX"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeY'",
                ],
                "PAS",
            ),
        ),
    )
    def test_multiple_test_cfgs(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        """
        Make optional fields in default schema required for `test_dataset_type`.
        Make sure PAS fixture from previous test still passes.
        """
        from ome_tiff_field_validator import OmeTiffFieldValidator

        validator = self.validator(test_data_fname, assay_type, tmp_path)
        validator.schema_regex_mapping[
            Path(__file__).parent.parent / "test_data/test_ome_tiff_field_schema_default.xsd"
        ] = ["test_dataset_type"]
        validator.get_schemas()
        errors = validator.collect_errors(coreuse=4)[:]
        validator.schema_regex_mapping = OmeTiffFieldValidator.schema_regex_mapping
        validator.get_schemas()
        self.check_errors(msg_re_list, errors)
