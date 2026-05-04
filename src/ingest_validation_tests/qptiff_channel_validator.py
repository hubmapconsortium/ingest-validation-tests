import os
import re
from pathlib import Path
from xml.etree import ElementTree

import pandas as pd
import tifffile
from tests_utils import get_non_global_paths_by_row
from validator import Validator


class QpTiffChannelValidator(Validator):
    description = """Check qptiff.channels.csv for cell/nuclei segmentation markers;
    check channels in QPTIFF against channels in qptiff.channels.csv"""
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
        self.shared_upload = any([bool("global" in str(path)) for path in self.paths])

    def _collect_errors(self) -> list[str | None]:
        if not (file_pairs_to_test := self.get_file_pairs_to_test()):
            self.errors.append(
                f"Could not find qptiff.channels.csv and associated QPTIFF files (required for {self.assay_type})."
            )
            return self._return_result(self.errors, False)
        for channels_csv, qptiff_file in file_pairs_to_test.items():
            self.check_qptiff_channels_file(channels_csv)
            self.check_channels(channels_csv, qptiff_file)
        return self._return_result(self.errors, bool(file_pairs_to_test))

    def get_file_pairs_to_test(self) -> dict:
        """
        For each data path, pair {qptiff.channels.csv: qptiff_file}
        """
        file_pairs_to_test = {}

        for path in self.paths:
            if self.shared_upload and "non_global" in str(path):
                file_pairs_to_test.update(self._get_non_global_file_pairs())
                continue

            channels_parent_path, qptiff_parent_path = self._get_parent_dir_paths(path)
            if not channels_parent_path or not qptiff_parent_path:
                continue

            channel_csv = self._get_file_path(channels_parent_path, "qptiff.channels.csv")
            qptiff_file = self._get_file_path(qptiff_parent_path, ".qptiff")
            if not (channel_csv and qptiff_file):
                continue
            file_pairs_to_test[channel_csv] = qptiff_file

        return file_pairs_to_test

    def _get_non_global_file_pairs(self) -> dict:
        """
        non_global directory may contain multiple raw/images/.*qptiff
        and lab_processed/images/.*channels.csv files in the same directory.
        Read the non_global_paths field of the metadata.tsv and retrieve
        pairs from there.

        For each row, pair {qptiff.channels.csv: qptiff_file}
        """
        if not self.schema or not self.schema.rows:
            raise Exception("TODO")
        pairs = {}
        non_global_paths = get_non_global_paths_by_row(self.schema.rows)
        for path_list in non_global_paths.values():
            # find qptiff and channels files in files list for row
            qptiff_regex = r"raw\/images\/[^\/]*qptiff"
            channels_regex = r"lab_processed\/images\/.*channels\.csv"
            qptiff_paths = [path for path in path_list if re.match(qptiff_regex, str(path))]
            channels_paths = [path for path in path_list if re.match(channels_regex, str(path))]

            # should be exactly one of each
            if len(qptiff_paths) != 1 or len(channels_paths) != 1:
                self.errors.append("TODO")
                continue

            # make sure paths exist
            for path in [channels_paths[0], qptiff_paths[0]]:
                if not Path(path).exists():
                    self.errors.append("TODO")

            pairs[channels_paths[0]] = qptiff_paths[0]
        return pairs

    def check_qptiff_channels_file(self, filename: Path):
        """
        Check for presence of at least one "Yes" value in
        'is_channel_used_for_nuclei_segmentation' and 'is_channel_used_for_cell_segmentation',
        and make sure columns are in order.
        """

        df = pd.read_csv(filename)
        # pipeline uses column position to determine channel & cell/nucleus segmentation
        if column_order_errors := self.check_column_order(df, filename):
            # validation can't continue if columns out of order
            self.errors.extend(column_order_errors)
            return
        # pipeline requires one or more y/Yes or t/True values in is_channel_used_for... fields
        for column in [df.columns[1], df.columns[2]]:
            if not any([val for val in df[column] if str(val).lower() in ["yes", "true"]]):
                self.errors.append(
                    f"{self.rel_filename_str(filename)} must have at least one 'Yes' value in column '{column}'"
                )

    def check_column_order(self, df: pd.DataFrame, filename: Path) -> list:
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

    def check_channels(self, channels_csv: Path, qptiff_file: Path):
        """
        Check that channels in channel_id column of qptiff.channels.csv
        match channels in accompanying QPTIFF file.
        """
        channels = pd.read_csv(channels_csv)
        channels_list = channels.iloc[:, 0].tolist()
        qptf_channels = self._get_qptiff_channels(qptiff_file)
        channels_list.sort()
        channels_set = set([str(channel) for channel in channels_list])
        if not channels_set == qptf_channels:
            self.errors.append(
                f"""Channels in {self.rel_filename_str(channels_csv)} and {self.rel_filename_str(qptiff_file)} do not match.
                    Channels in CSV that are not present in QPTIFF: {', '.join(channels_set.difference(qptf_channels))}
                    Channels in QPTIFF that are not present in CSV: {', '.join(channels.difference(channels_set))}
                """
            )

    def _get_qptiff_channels(self, qptiff_file: Path) -> set[str]:
        qptf_channels = []
        with tifffile.TiffFile(qptiff_file) as qptf:
            for page in qptf.pages:
                if description := page.tags.get("ImageDescription").value:
                    """
                    Bioformats conversion (used in pipeline) uses ImageDescription.Biomarker
                    as the channel name if present, defaulting to ImageDescription.Name if not.
                    https://github.com/ome/bioformats/blob/877c317e4e396381dc76e56c1539b24947f71dce/components/formats-gpl/src/loci/formats/in/VectraReader.java#L546
                    """
                    if (
                        biomarker := ElementTree.fromstring(description).find("Biomarker")
                    ) is not None:
                        qptf_channels.append(biomarker.text)
                    elif (
                        channel_name := ElementTree.fromstring(description).find("Name")
                    ) is not None:
                        qptf_channels.append(channel_name.text)
        return set(sorted([str(channel) for channel in qptf_channels]))

    def _get_parent_dir_paths(self, path) -> tuple[Path | None, Path | None]:
        channels_parent_path = Path(os.path.join(path, "lab_processed/images"))
        if not channels_parent_path.exists():
            channels_parent_path = None
            # we don't know where the files will be in a shared upload, don't report if missing;
            # directory validation should have caught a total omission
            if not self.shared_upload:
                self.errors.append(
                    f"Can't find 'lab_processed/images' subdirectory in '{path.stem}'."
                )
        qptiff_parent_path = Path(os.path.join(path, "raw/images"))
        if not qptiff_parent_path.exists():
            qptiff_parent_path = None
            if not self.shared_upload:
                self.errors.append(f"Can't find 'raw/images' subdirectory in '{path.stem}'.")
        return channels_parent_path, qptiff_parent_path

    def _get_file_path(self, parent_dir_path: Path, search_str: str) -> Path | None:
        files = []
        for filename in parent_dir_path.iterdir():
            if search_str in str(filename).lower():
                files.append(filename)
        if len(files) != 1:
            self.errors.append(
                f"Found {len(files)} {search_str} files in {parent_dir_path} directory."
            )
            return
        return files[0]
