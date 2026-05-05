import os
import shlex
import tempfile
from multiprocessing import Pool
from pathlib import Path
from subprocess import check_call

import pandas as pd
import tifffile
import xmlschema
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

    def _collect_errors(self) -> list[str | None]:
        parent_dir = os.path.commonpath(self.paths)
        with tempfile.TemporaryDirectory(dir=parent_dir) as tmp_dir:
            self.tmp_dir = Path(tmp_dir)

            # compile test files, including converted OME-TIFFs
            try:
                self.files_to_test = self.get_files_to_test()
            except Exception as e:
                if not self.errors:
                    self.errors.append(f"Error retrieving files to test: {e}")
                return self._return_result(self.errors, False)

            if not self.files_to_test:
                self.errors.append(
                    f"Could not find qptiff.channels.csv and associated QPTIFF files (required for {self.assay_type})."
                )
                return self._return_result(self.errors, False)

            # check channels CSV format and verify channels against converted QPTIFF
            for files_dict in self.files_to_test.values():
                self.check_qptiff_channels_file(files_dict["csv"])
                self.check_channels(files_dict)
            return self._return_result(self.errors, bool(self.files_to_test))

    def get_files_to_test(self) -> dict[Path, dict[str, Path]]:
        """
        For each data path, locate channels CSV and QPTIFF file.
        Parallelize convering QPTIFF -> OME.TIFF, adding to
        tmp_dir/data_dir.
        Retrieve converted files.
        Returns:
            {<data_path>: {
                "csv": <csv_path>,
                "qptiff": <qptiff_path>,
                "ome_tiff": <ome_tiff_path>}}
        """
        files_to_test = {}

        for path in self.paths:
            channels_parent_path, qptiff_parent_path = self._get_parent_dir_paths(path)
            if not channels_parent_path or not qptiff_parent_path:
                continue

            channel_csv = self._get_file_path(channels_parent_path, "qptiff.channels.csv")
            qptiff_file = self._get_file_path(qptiff_parent_path, ".qptiff")
            if not (channel_csv and qptiff_file):
                continue
            files_to_test[path] = {"csv": channel_csv, "qptiff": qptiff_file}

        try:
            self.convert_files(files_to_test)
        except Exception as e:
            self.errors.append(f"Error converting QPTIFFs to OME-TIFFs: {e}")
            raise

        for path, files in files_to_test.items():
            ome_tiff_path = output_filename_template(self.tmp_dir, path, files["qptiff"])
            if not ome_tiff_path.exists():
                self.errors.append(
                    f"Error with converted OME-TIFF file '{self.rel_filename_str(ome_tiff_path)}': does not exist"
                )
            files_to_test[path]["ome_tiff"] = ome_tiff_path

        return files_to_test

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

    def check_channels(self, file_dict: dict[str, Path]):
        """
        Check that channels in channel_id column of qptiff.channels.csv
        match channels in accompanying OME.TIFF file.
        """
        # get channels from CSV channel_id field
        channels = pd.read_csv(file_dict["csv"])
        channels_list = channels.iloc[:, 0].tolist()
        channels_list.sort()
        channels_set = set(channels_list)
        # get channels from OME-TIFF
        try:
            tiff_channels = self._get_tiff_channels(file_dict["ome_tiff_file"])
        except Exception as e:
            self.errors.append(f"Error with {file_dict['qptiff']}: {e}")
        if not channels_set == tiff_channels:
            self.errors.append(
                f"""Channels in {self.rel_filename_str(file_dict["csv"])}
                    don't match those in QPTIFF {self.rel_filename_str(file_dict["qptiff_file"])}
                    (as converted to OME-TIFF).

                    Channels in CSV not present in QPTIFF:
                    {', '.join(channels_set.difference(tiff_channels))}

                    QPTIFF channels:
                    {', '.join(tiff_channels)}
                """
            )

    def _get_tiff_channels(self, tiff_file: Path) -> set[str]:
        with tifffile.TiffFile(tiff_file) as tf:
            ome_metadata = tf.ome_metadata
            if not ome_metadata:
                raise Exception(f"Error retrieving OME-XML for converted file {tiff_file}.")
            ome_xml = xmlschema.XmlDocument(ome_metadata)
        try:
            channels = ome_xml.find("Image").find("Pixels").findall("Channel")  # type: ignore
        except AttributeError:
            raise Exception(f"Error retrieving channels from converted file {tiff_file}.")
        channel_names_and_ids = []
        for channel in channels:
            channel_names_and_ids.append(channel.get("Name"))
            channel_names_and_ids.append(channel.get("ID"))
        channel_names_and_ids.sort()
        return set(channel_names_and_ids)

    def _get_parent_dir_paths(self, path) -> tuple[Path | None, Path | None]:
        channels_parent_path = Path(os.path.join(path, "lab_processed/images"))
        if not channels_parent_path.exists():
            channels_parent_path = None
            self.errors.append(f"Can't find 'lab_processed/images' subdirectory in '{path.stem}'.")
        qptiff_parent_path = Path(os.path.join(path, "raw/images"))
        if not qptiff_parent_path.exists():
            qptiff_parent_path = None
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

    def convert_files(self, files_to_test: dict[str, dict[str, Path]]) -> list[str]:
        # keys are filepaths, get data_path dirname (path.stem) to construct output path
        # parallelize file conversion, write to tmp_dir/path.stem/ for each file
        # collect any errors in rslt_list
        pool = Pool(self.threads)
        try:
            rslt_list: list[str] = list(
                rslt
                for rslt in pool.starmap(
                    convert_qptiff,
                    [(key, value, self.tmp_dir) for key, value in files_to_test.items()],
                )
                if rslt is not None
            )
            return rslt_list
        except Exception as e:
            self._log(f"Error {e}")
            raise
        finally:
            pool.close()
            pool.join()


bioformats2raw_path = ""
raw2ometiff_path = ""


def output_filename_template(
    tmp_dir: Path, data_dir_path: Path, qptiff_path: Path, extension: str = "ome.tiff"
) -> Path:
    parent_dir = Path(f"{tmp_dir}/{data_dir_path.stem}")
    if not parent_dir.exists():
        parent_dir.mkdir()
    return Path(parent_dir / f"{qptiff_path.stem}_converted.{extension}")


def convert_qptiff(path: Path, files_dict: dict[str, Path], tmp_dir: Path) -> str | None:
    """
    The conversion logic is borrowed from the phenocycler pipeline (v1.4.8);
    major changes to how QPTIFFs are converted in the pipeline will
    desync this validation.
    """
    # construct raw output path: tmp_dir/data_dir_name/qptiff_name_converted.raw
    raw_output_path = output_filename_template(tmp_dir, path, files_dict["qptiff"], "raw")
    bioformats2raw_command = [
        bioformats2raw_path,
        "--resolutions",
        "1",
        "--series",
        "0",
        files_dict["qptiff"],  # input file
        raw_output_path,  # output file
    ]
    # construct ome_tiff output path: tmp_dir/data_dir_name/qptiff_name_converted.ome.tiff
    ome_tiff_output_path = output_filename_template(tmp_dir, path, files_dict["qptiff"])
    raw2ometiff_command = [
        raw2ometiff_path,
        raw_output_path,  # input file
        ome_tiff_output_path,  # output file
    ]
    try:
        print("Running", shlex.join(bioformats2raw_command))
        check_call(bioformats2raw_command)
    except Exception as e:
        return f"Error in bioformats2raw conversion: {e}"
    try:
        print("Running", raw2ometiff_command)
        check_call(raw2ometiff_command)
    except Exception as e:
        return f"Error in raw2ometiff conversion: {e}"
