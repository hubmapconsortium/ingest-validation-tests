import re
import zipfile
from pathlib import Path

import jsonschema
import pytest


class TestOmeTiffFieldValidator:

    def validator(self, test_data_fname, assay_type, tmp_path):
        from ome_tiff_field_validator import OmeTiffFieldValidator

        test_data_path = Path(test_data_fname)
        zfile = zipfile.ZipFile(test_data_path)
        zfile.extractall(tmp_path)
        return OmeTiffFieldValidator(tmp_path / test_data_path.stem, assay_type)

    def check_errors(self, msg_re_list, errors):
        assert len(msg_re_list) == len(errors)
        unmatched_errors = []
        for err_str in errors:
            msg_re_list_dup = list(msg_re_list)  # to avoid editing during iteration
            match = False
            for re_str in msg_re_list_dup:
                if (err_str is None and re_str is None) or re.fullmatch(
                    re_str, err_str, flags=re.MULTILINE
                ):
                    msg_re_list.remove(re_str)
                    match = True
                    break
            if not match:
                unmatched_errors.append(err_str)
        assert not unmatched_errors, f"Unmatched errors: {unmatched_errors}"

    @pytest.mark.parametrize(
        ("test_data_fname", "msg_re_list", "assay_type"),
        (
            (
                "test_data/codex_tree_ometiff_bad.zip",
                [
                    ".*/codex_tree_ometiff_bad/tubhiswt_C0_bad.ome.tif is not a valid OME.TIFF file: Failed to read OME XML",
                    ".*/codex_tree_ometiff_bad/sample1.ome.tif is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeXUnit'; missing required attribute 'PhysicalSizeY'; missing required attribute 'PhysicalSizeYUnit'",
                    ".*/codex_tree_ometiff_bad/sample2.ome.tif is not a valid OME.TIFF file per schema 'ome_tiff_field_schema_default.xsd': missing required attribute 'PhysicalSizeX'; missing required attribute 'PhysicalSizeXUnit'; missing required attribute 'PhysicalSizeY'; missing required attribute 'PhysicalSizeYUnit'",
                ],
                "CODEX",
            ),
            ("test_data/codex_tree_ometiff_good.zip", [], "CODEX"),
            ("test_data/fake_snrnaseq_tree_good.zip", [], "snRNAseq"),
            (
                "test_data/complex_small_ome_tiff.zip",
                [
                    ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not a valid OME.TIFF file: PhysicalSizeX is required but missing; PhysicalSizeY is required but missing"
                ],
                "PAS",
            ),
        ),
    )
    def test_ome_tiff_field_validator(self, test_data_fname, msg_re_list, assay_type, tmp_path):
        validator = self.validator(test_data_fname, assay_type, tmp_path)
        errors = validator.collect_errors(coreuse=4)[:]
        errors = list(err for err in errors if err is not None)
        self.check_errors(msg_re_list, errors)

    test_config_entry = {
        "name": "test requirements",
        "re": "test_dataset_type",
        "fields": {
            "Pixels_PhysicalSizeZ": {
                "dtype": "float",
                "msg": "dtype should be float",
                "required_field": True,
            },
            "Pixels_PhysicalSizeZUnit": {
                "dtype": "categorical",
                "allowed_values": ["µm"],
                "required_field": True,
            },
        },
    }

    # @pytest.mark.parametrize(
    #     ("test_data_fname", "msg_re_list", "assay_type"),
    #     (
    #         ("test_data/fake_snrnaseq_tree_good.zip", [], "test_dataset_type"),
    #         (
    #             "test_data/complex_small_ome_tiff.zip",
    #             [
    #                 ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not"
    #                 " a valid OME.TIFF file: Pixels_PhysicalSizeX is required but missing;"
    #                 " Pixels_PhysicalSizeY is required but missing;"
    #                 " Pixels_PhysicalSizeZ is required but missing.*"
    #             ],
    #             "test_dataset_type",
    #         ),
    #         (
    #             "test_data/complex_small_ome_tiff.zip",
    #             [
    #                 ".*complex_small_ome_tiff/917_cropped_0_Z0_C3_T0.ome.tiff is not"
    #                 " a valid OME.TIFF file: Pixels_PhysicalSizeX is required but missing;"
    #                 " Pixels_PhysicalSizeY is required but missing*"
    #             ],
    #             "PAS",
    #         ),
    #     ),
    # )
    # def test_multiple_test_cfgs(self, test_data_fname, msg_re_list, assay_type, tmp_path):
    #     """
    #     Make optional fields in default schema required for `test_dataset_type`.
    #     Make sure PAS fixture from previous test still passes.
    #     """
    #     validator = self.validator(test_data_fname, assay_type, tmp_path)
    #     validator.cfg_list.append(self.test_config_entry)
    #     jsonschema.validate(validator.cfg_list, validator.schema)
    #     errors = validator.collect_errors(coreuse=4)[:]
    #     self.check_errors(msg_re_list, errors)
