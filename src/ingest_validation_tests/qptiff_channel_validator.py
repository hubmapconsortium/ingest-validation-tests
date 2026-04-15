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

    ordered_columns = [
        ["channel_id", "channel id"],
        [
            "is_channel_used_for_nuclei_segmentation",
            "is channel used for nuclei segmentation",
        ],
        [
            "is_channel_used_for_cell_segmentation",
            "is channel used for cell segmentation",
        ],
        ["is_antibody", "is antibody"],
    ]

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.errors = []

    def _collect_errors(self) -> list[str | None]:
        filenames_to_test = []

        for path in self.paths:
            images_path = Path(os.path.join(path, "lab_processed/images"))
            if not images_path.exists():
                self.errors.append(
                    f"Can't find 'lab_processed/images' subdirectory in '{path.stem}'."
                )
                continue
            for filename in images_path.iterdir():
                if "qptiff.channels.csv" in str(filename).lower():
                    filenames_to_test.append(filename)

        if filenames_to_test:
            for filename in filenames_to_test:
                self.check_qptiff_file(filename)
        else:
            self.errors.append(
                f"Could not find 'lab_processed/images/*.qptiff.channels.csv' files (required for {self.assay_type})."
            )
        return self._return_result(self.errors, filenames_to_test)

    def check_qptiff_file(self, filename: Path):
        df = pd.read_csv(filename)
        # pipeline uses column position to determine channel & cell/nucleus segmentation
        if column_order_errors := self._check_column_order(df, filename):
            # validation can't continue if columns out of order
            self.errors.extend(column_order_errors)
            return
        # pipeline requires one or more y/Yes or t/True values in is_channel_used_for... fields
        for column in [df.columns[1], df.columns[2]]:
            if not any([val for val in df[column] if str(val).lower() in ["yes", "true"]]):
                self.errors.append(
                    f"{self.rel_filename_str(filename)} must have at least one 'Yes' value in column '{column}'"
                )

    def _check_column_order(self, df: pd.DataFrame, filename: Path) -> list:
        column_order_errors = []
        for index, columns in enumerate(self.ordered_columns):
            try:
                # ensure columns 0-3 match ordered_columns, ignore any additional columns
                assert df.columns[index] in columns, f"'{columns[0]}' must be column {index+1}"
            except AssertionError as e:
                # if a column is out of place, first check whether it is a required column or not
                if df.columns[index] not in [
                    column_name for columns in self.ordered_columns for column_name in columns
                ]:
                    # columns hopelessly out of order if non-required column found, just report that
                    return [
                        f"Unexpected column header found in column {index+1}: '{df.columns[index]}'. Columns 1-4 must match required order. Can't validate {self.rel_filename_str(filename)}."
                    ]
                else:
                    column_order_errors.append(f"{self.rel_filename_str(filename)}: {e}")
        return column_order_errors
