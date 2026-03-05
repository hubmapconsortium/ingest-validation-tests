import os
from pathlib import Path

import pandas as pd
from validator import Validator


class QpTiffChannelValidator(Validator):
    description = """Check for presence of at least one "Yes" value in
    'is_channel_used_for_nuclei_segmentation' and 'is_channel_used_for_cell_segmentation'"""
    cost = 1.0
    version = "1.0"
    required = ["phenocycler"]

    def _collect_errors(self) -> list[str | None]:
        filenames_to_test = []
        rslt_list = []

        for path in self.paths:
            images_path = Path(os.path.join(path, "lab_processed/images"))
            if not images_path.exists():
                rslt_list.append(
                    f"Can't find 'lab_processed/images' subdirectory in '{path.stem}'."
                )
                continue
            for filename in images_path.iterdir():
                if "qptiff.channels.csv" in str(filename).lower():
                    filenames_to_test.append(filename)

        if not filenames_to_test:
            rslt_list.append(
                f"Could not find 'lab_processed/images/*.qptiff.channels.csv' files (required for {self.assay_type})."
            )
            return rslt_list

        for filename in filenames_to_test:
            if err := self.check_qptiff_file(filename):
                rslt_list.extend(err)
        return self._return_result(rslt_list, filenames_to_test)

    def check_qptiff_file(self, filename: Path) -> list:
        errors = []
        df = pd.read_csv(filename)
        # guidance file uses spaces in fieldnames, normalize
        df = df.rename(
            columns={
                "is channel used for nuclei segmentation": "is_channel_used_for_nuclei_segmentation",
                "is channel used for cell segmentation": "is_channel_used_for_cell_segmentation",
            }
        )
        for column in [
            "is_channel_used_for_nuclei_segmentation",
            "is_channel_used_for_cell_segmentation",
        ]:
            if column not in df:
                errors.append(
                    f"{self.rel_filename_str(filename)} is missing required column '{column}'"
                )
            # guidance file specifies "Yes" or "No"
            elif not any([val for val in df[column] if val == "Yes"]):
                errors.append(
                    f"{self.rel_filename_str(filename)} must have at least one 'Yes' value in column '{column}'"
                )
        return list(set(errors))
