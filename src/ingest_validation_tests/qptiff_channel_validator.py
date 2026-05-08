import shutil
import subprocess
import tempfile
from datetime import datetime
from functools import cached_property
from multiprocessing import Pool
from pathlib import Path
from textwrap import dedent
from typing import Literal

import pandas as pd
import tifffile
import xmlschema
from validator import Validator

BIOFORMATS2RAW_PATH = Path("/hive/users/hive/bioformats2raw-0.12.0/bin")
RAW2OMETIFF_PATH = Path("/hive/users/hive/raw2ometiff-0.10.0/bin")


class QpTiffChannelValidator(Validator):
    description = (
        "Check qptiff.channels.csv for cell/nuclei segmentation markers, correct column order"
    )
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
    tmp_suffix = "_tmp_ome_tiffs"

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.errors = []

    def _collect_errors(self) -> list[str | None]:
        try:
            self.files_to_test
            assert (
                self.files_to_test
            ), f"Could not find qptiff.channels.csv and associated QPTIFF files (required for {self.assay_type})."
        except Exception as e:
            if not self.errors:
                self.errors.append(f"Error retrieving files to test: {e}")
        else:
            self._run_validation()
        return self._return_result(self.errors, bool(self.files_to_test))

    def _run_validation(self):
        for files_dict in self.files_to_test.values():
            # check channels CSV format
            self.check_qptiff_channels_file(files_dict["csv"])

    @cached_property
    def files_to_test(self) -> dict[Path, dict[str, Path]]:
        """
        For each data path, locate channels CSV and QPTIFF file.
        Returns:
            {<data_path>: {
                "csv": <csv_path>,
                "qptiff": <qptiff_path>,
                }}
        """
        files_to_test = {}

        for path in self.paths:
            channel_csv = self._get_file_path(path / "lab_processed/images", ".channels.csv")
            qptiff_file = self._get_file_path(path / "raw/images", ".qptiff")
            if not (channel_csv and qptiff_file):
                continue
            files_to_test[path] = {"csv": channel_csv, "qptiff": qptiff_file}

        return files_to_test

    ##################
    # CSV validation #
    ##################

    def check_qptiff_channels_file(self, filename: Path):
        """
        Check for presence of at least one "Yes" value in
        'is_channel_used_for_nuclei_segmentation' and 'is_channel_used_for_cell_segmentation',
        and make sure columns are in order.
        """

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

    def _get_file_path(self, parent_path: Path, extension: str) -> Path | None:
        if not parent_path.exists():
            self.errors.append(
                f"Did not find expected directory {self.rel_filename_str(parent_path)}"
            )
            return
        files = []
        for filename in parent_path.iterdir():
            if str(filename).lower().endswith(extension):
                files.append(filename)
        if len(files) != 1:
            self.errors.append(
                f"Found {len(files)} {extension} files in {self.rel_filename_str(parent_path)} directory."
            )
            return
        return files[0]

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


class QpTiffChannelComparisonValidator(QpTiffChannelValidator):
    description = "Check channels in QPTIFF against channels in qptiff.channels.csv"

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.tmp_dir = self.get_tmp_dir()

    def get_tmp_dir(self):
        if not self.scratch_dir:
            self.errors.append("No base path for scratch directory provided.")
            return
        # to avoid re-converting files in subsequent validation runs,
        # check if a tmp_dir has already been created for this upload
        # (tmp_dir can be manually deleted if necessary)
        if (
            len(existing_dirs := list(self.scratch_dir.glob(f"{self.uuid}*{self.tmp_suffix}")))
            == 1
        ):
            print(f"Existing temp dir found: {existing_dirs[0]}")
            self.tmp_dir = existing_dirs[0]
        else:
            self.tmp_dir = Path(
                tempfile.mkdtemp(
                    prefix=f"{self.uuid}_", suffix=self.tmp_suffix, dir=self.scratch_dir
                )
            )

    def _collect_errors(self):
        # TODO: temp dir will not be deleted if _collect_errors not called
        try:
            assert not self.errors, "Errors found!"
            super()._collect_errors()
        except Exception as e:
            self.errors.append(f"Error testing files: {e}")
        finally:
            self._cleanup()
            return self._return_result(self.errors, True)

    def _run_validation(self):
        # verify channels against converted QPTIFF
        pool = Pool(self.threads)
        try:
            rslt_list: list[str] = list(
                rslt
                for rslt in pool.imap_unordered(
                    self.check_channels,
                    list(self.files_to_test.values()),
                )
                if rslt is not None
            )
            self.errors.extend(rslt_list)
        except Exception as e:
            self._log(f"Error {e}")
            raise
        finally:
            pool.close()
            pool.join()

    @cached_property
    def files_to_test(self):
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
        assert self.tmp_dir
        files_to_test = super().files_to_test
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

    #######################
    # Channels validation #
    #######################

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
            tiff_channels = self._get_tiff_channels(file_dict["ome_tiff"])
        except Exception as e:
            return f"Error with {self.rel_filename_str(file_dict['qptiff'])}: {e}"
        if channels_set.difference(tiff_channels):
            return dedent(
                f"""Channels in {self.rel_filename_str(file_dict["csv"])}
                    don't match those in QPTIFF {self.rel_filename_str(file_dict["qptiff"])}
                    (as converted to OME-TIFF).

                    Channels in CSV not present in QPTIFF:
                    {', '.join(channels_set.difference(tiff_channels))}

                    QPTIFF channels:
                    {', '.join(tiff_channels)}
                """
            ).strip()

    def _get_tiff_channels(self, tiff_file: Path) -> set[str]:
        print(f"({timestamp()}) Retrieving channels from {tiff_file}...")
        with tifffile.TiffFile(tiff_file) as tf:
            if (
                not (ome_metadata := tf.ome_metadata)
                or not (ome_xml := xmlschema.XmlDocument(ome_metadata))
                or not ome_xml.schema
            ):
                raise Exception(f"Error retrieving OME-XML for converted file {tiff_file}.")
        channel_names_and_ids = []
        try:
            ome_channels = (
                ome_xml.schema.to_dict(ome_xml).get("Image")[0].get("Pixels").get("Channel")  # type: ignore
            )
            for channel in ome_channels:
                channel_names_and_ids.extend(
                    [
                        channel_attr
                        for channel_attr in [channel.get("@ID"), channel.get("@Name")]
                        if channel_attr is not None
                    ]
                )
        except AttributeError:
            raise Exception(
                f"Error retrieving channels from converted file {self.rel_filename_str(tiff_file)}."
            )
        channel_names_and_ids.sort()
        print(f"({timestamp()}) Channels found for {self.rel_filename_str(tiff_file)}")
        return set(channel_names_and_ids)

    def _get_file_path(self, parent_path: Path, extension: str) -> Path | None:
        if not parent_path.exists():
            self.errors.append(
                f"Did not find expected directory {self.rel_filename_str(parent_path)}"
            )
            return
        files = []
        for filename in parent_path.iterdir():
            if str(filename).lower().endswith(extension):
                files.append(filename)
        if len(files) != 1:
            self.errors.append(
                f"Found {len(files)} {extension} files in {self.rel_filename_str(parent_path)} directory."
            )
            return
        return files[0]

    def check_dependencies(self):
        msg = ""
        if not BIOFORMATS2RAW_PATH.exists():
            msg += "bioformats2raw not installed"
        if not RAW2OMETIFF_PATH.exists():
            msg += "raw2ometiff not installed" if not msg else "; raw2ometiff not installed"
        return msg

    def convert_files(self, files_to_test: dict[Path, dict[str, Path]]):
        # keys are filepaths, get data_path dirname (path.stem) to construct output path
        # parallelize file conversion, write to tmp_dir/path.stem/ for each file
        # collect any errors in rslt_list
        if error := self.check_dependencies():
            self.errors.append(error)
            return
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
            self.errors.extend(rslt_list)
        except Exception as e:
            self._log(f"Error {e}")
            raise
        finally:
            pool.close()
            pool.join()

    def _cleanup(self):
        # if validation was successful, delete tmp_dir;
        # otherwise, retain to avoid re-converting files.
        # handle case where tmp_dir was not created in the
        # first place.
        msg = ""
        if self.errors:
            msg = "Errors found!"
            if self.tmp_dir:
                msg += f" Retaining temp dir {self.tmp_dir}"
            else:
                msg += " No tmp_dir found."
        # no errors but also no tmp_dir?
        elif not self.tmp_dir:
            self.errors.append("Validation passed but no tmp_dir found?")
        # make sure this is the right directory
        elif not (self.uuid in str(self.tmp_dir)) and not (
            self.tmp_dir.stem.endswith(self.tmp_suffix)
        ):
            print(f"Unexpected temp dir name (found {self.tmp_dir}), not deleting")
        else:
            print(f"Validation passed, removing temp dir {self.tmp_dir}")
            shutil.rmtree(self.tmp_dir)


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
    raw_output_path = output_filename_template(tmp_dir, path, files_dict["qptiff"], "raw")
    bioformats2raw_command = [
        BIOFORMATS2RAW_PATH,
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
        RAW2OMETIFF_PATH,
        raw_output_path,  # input file
        ome_tiff_output_path,  # output file
    ]
    try:
        convert_file("bioformats2raw", raw_output_path, bioformats2raw_command)
        convert_file("raw2ometiff", ome_tiff_output_path, raw2ometiff_command)
    except Exception as e:
        return str(e)


def timestamp():
    return datetime.now().strftime("%H:%M:%S")


def convert_file(
    step: Literal["bioformats2raw", "raw2ometiff"], output_path: Path, command: list[Path] | str
):
    try:
        # we don't want to automatically re-convert
        if output_path.exists():
            print(f"{step} file conversion output already exists: {output_path}; skipping step")
            pass
        else:
            print(f"({timestamp()}) Running ", command)
            subprocess.check_call(command)
            print(f"({timestamp()}) Finished {output_path}")
    except Exception as e:
        raise Exception(f"Error in {step} conversion: {e}")
