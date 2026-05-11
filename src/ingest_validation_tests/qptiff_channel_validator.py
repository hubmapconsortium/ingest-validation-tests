import subprocess
import tempfile
from functools import cached_property
from multiprocessing import Pool
from pathlib import Path
from textwrap import dedent

import pandas as pd
import xmlschema
from validator import Validator, get_rel_filename_str

BIOFORMATS2RAW_PATH = Path("/hive/users/hive/bioformats2raw-0.12.0/bin/bioformats2raw")


class QpTiffChannelValidator(Validator):
    version = "1.0"
    cost = 1.0
    required = ["phenocycler"]
    description = (
        "Check qptiff.channels.csv for cell/nuclei segmentation markers, correct column order"
    )

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
        try:
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

    ###################
    # File collection #
    ###################

    @cached_property
    def files_to_test(self) -> dict[Path, dict[str, Path]]:
        """
        For each data path, locate channels CSV and QPTIFF file.
        Returns:
            {<data_path>: {
                "csv": <csv_path>,
                "qptiff": <qptiff_path>}}
        """
        files_to_test = {}

        for path in self.paths:
            channel_csv = self._get_file_path(path / "lab_processed/images", ".channels.csv")
            qptiff_file = self._get_file_path(path / "raw/images", ".qptiff")
            if not (channel_csv and qptiff_file):
                continue
            files_to_test[path] = {"csv": channel_csv, "qptiff": qptiff_file}

        return files_to_test

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

    ##################
    # CSV Validation #
    ##################

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


class QpTiffChannelComparisonValidator(QpTiffChannelValidator):
    cost = 5.0
    description = "Check channels in QPTIFF against channels in qptiff.channels.csv"

    def _collect_errors(self):
        if not self.scratch_dir:
            self.errors.append("No base path for scratch directory provided.")
        else:
            with tempfile.TemporaryDirectory(
                dir=self.scratch_dir, prefix=self.uuid, suffix="_ome_xml"
            ) as td:
                self.tmp_dir = Path(td)
                try:
                    super()._collect_errors()
                except Exception as e:
                    self.errors.append(f"Error testing files: {e}")
        return self._return_result(self.errors, True)

    def _run_validation(self):
        pool = Pool(self.threads)
        engine = Engine()
        try:
            rslt_list: list[str] = list(
                rslt
                for rslt in pool.starmap(
                    engine,
                    [
                        (data_path, file_dict, self.tmp_dir)
                        for data_path, file_dict in self.files_to_test.items()
                    ],
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


class Engine:
    def __call__(self, data_path: Path, file_dict: dict[str, Path], tmp_dir: Path) -> str | None:
        """
        Check that channels in channel_id column of qptiff.channels.csv
        match channels in accompanying OME-XML file (generated from QPTIFF).
        """
        self.tmp_dir = tmp_dir
        try:
            csv_channels = self.get_csv_channels(file_dict["csv"])
            qptiff_channels = self.get_qptiff_channels(data_path, file_dict["qptiff"])
            if csv_channels.difference(qptiff_channels):
                return dedent(
                    f"""Channels in {get_rel_filename_str(data_path, file_dict["csv"])}
                        don't match those in QPTIFF {get_rel_filename_str(data_path, file_dict["qptiff"])}
                        (from converted OME-XML).

                        Channels in CSV not present in QPTIFF:
                        {', '.join(csv_channels.difference(qptiff_channels))}

                        QPTIFF channels:
                        {', '.join(qptiff_channels)}
                    """
                ).strip()
        except Exception as e:
            return str(e)

    def get_csv_channels(self, csv_path: Path) -> set[str]:
        # get channels from CSV channel_id field
        channels = pd.read_csv(csv_path)
        channels_list = channels.iloc[:, 0].tolist()
        channels_list.sort()
        return set([str(channel) for channel in channels_list])

    def get_qptiff_channels(self, data_path: Path, qptiff_path: Path):
        # get OME-XML
        ome_xml_path = self.extract_ome_xml(data_path, qptiff_path)
        try:
            return self.get_ome_xml_channels(ome_xml_path)
        except Exception as e:
            raise Exception(f"Error with {get_rel_filename_str(data_path, qptiff_path)}: {e}")

    def extract_ome_xml(self, data_path: Path, qptiff_path) -> Path:
        """
        The bioformats2raw params (except for no-tiles) are borrowed from the
        phenocycler pipeline (v1.4.8); major changes to how QPTIFFs are converted
        in the pipeline may desync this validation.
        """
        raw_output_path = Path(self.tmp_dir / f"{data_path.stem}_{qptiff_path.stem}_converted")
        bioformats2raw_command = [
            BIOFORMATS2RAW_PATH,
            "--resolutions",
            "1",
            "--series",
            "0",
            "--no-tiles",  # do not convert image
            qptiff_path,  # input file
            raw_output_path,  # output file
        ]
        print(f"Running {bioformats2raw_command}")
        subprocess.check_call(bioformats2raw_command)
        ome_xml_path = Path(raw_output_path / "OME/METADATA.ome.xml")
        if not ome_xml_path.exists():
            raise Exception(f"Error with OME-XML file '{ome_xml_path}': does not exist")
        return ome_xml_path

    def get_ome_xml_channels(self, ome_xml_file: Path) -> set[str]:
        print(f"Retrieving channels from {ome_xml_file}...")
        if not (ome_xml := xmlschema.XmlDocument(ome_xml_file)) or not ome_xml.schema:
            raise Exception(f"Error retrieving OME-XML for converted file {ome_xml_file}.")
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
            raise Exception(f"Error retrieving channels from converted file {ome_xml_file}.")
        channel_names_and_ids.sort()
        print(f"Channels found for {ome_xml_file}")
        return set([str(channel) for channel in channel_names_and_ids])

    def check_dependencies(self):
        if not BIOFORMATS2RAW_PATH.exists():
            raise Exception("bioformats2raw not installed")
        elif not BIOFORMATS2RAW_PATH.is_file():
            raise Exception(f"bioformats2raw path is not a file: {BIOFORMATS2RAW_PATH}")
