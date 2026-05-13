import os
import re
import subprocess
from functools import cached_property
from multiprocessing import Pool
from pathlib import Path
from textwrap import dedent

import pandas as pd
import xmlschema
from validator import Validator, get_non_global_paths_by_row, get_rel_filename_str

# pipeline uses 0.9.2 but that does not include no-tiles arg
BIOFORMATS2RAW_RELEASE = "0.10.0"


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
        self.shared_upload = any(
            [bool(path.stem in ["global", "non_global"]) for path in self.paths]
        )
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
        finally:
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
            if self.shared_upload:
                files_to_test.update(self._get_shared_upload_file_pairs(path.parent))
                break
            channel_csv = self._get_file_path(path / "lab_processed/images", ".channels.csv")
            qptiff_file = self._get_file_path(path / "raw/images", ".qptiff")
            if not (channel_csv and qptiff_file):
                continue
            files_to_test[path] = {"csv": channel_csv, "qptiff": qptiff_file}

        return files_to_test

    def _get_shared_upload_file_pairs(self, base_path: Path) -> dict:
        """
        non_global directory may contain multiple raw/images/.*qptiff
        and lab_processed/images/.*channels.csv files.
        Read the non_global_paths field of the metadata.tsv and retrieve
        files from there. Check global directory if not found.
        Return {"data_path": {"csv": qptiff.channels.csv, "qptiff": qptiff_file}}
        """
        if not self.schema_rows:
            raise Exception("Row data from metadata.tsv is required to validate shared uploads.")
        files = {}
        # check non_global files identified in metadata.tsv
        non_global_paths = get_non_global_paths_by_row(self.schema_rows, base_path)
        for data_path, row_paths in non_global_paths.items():
            # find qptiff and channels files in non_global/
            qptiff_paths = [
                Path(base_path / path)
                for path in row_paths
                if re.search(r"raw\/images\/[^\/]*qptiff", str(path))
            ]
            channels_paths = [
                Path(base_path / path)
                for path in row_paths
                if re.search(r"lab_processed\/images\/.*channels\.csv", str(path))
            ]

            # if files are not found, check in global/
            qptiff_glob = "raw/images/*qptiff"
            channels_glob = "lab_processed/images/*channels.csv"
            if len(qptiff_paths) == 0:
                qptiff_paths = [file for file in Path(base_path / "global").glob(qptiff_glob)]
            if len(channels_paths) == 0:
                channels_paths = [file for file in Path(base_path / "global").glob(channels_glob)]

            # there should be exactly one of each, but if not...
            if len(qptiff_paths) != 1 or len(channels_paths) != 1:
                # give descriptive error message if a file is in the wrong place
                if bad_non_global_paths := self._get_error_for_bad_non_global_paths(
                    (qptiff_paths, qptiff_glob), (channels_paths, channels_glob), base_path
                ):
                    for error in bad_non_global_paths:
                        self.errors.append(error)
                else:
                    self.errors.append(
                        f"Found {len(qptiff_paths)} qptiffs and {len(channels_paths)} channels.csv paths for dataset {data_path} in shared upload."
                    )
                continue

            # make sure paths exist
            for path in [channels_paths[0], qptiff_paths[0]]:
                if not Path(path).exists():
                    self.errors.append(f"Path {self.rel_filename_str(path)} doesn't exist.")
            files[data_path] = {"csv": channels_paths[0], "qptiff": qptiff_paths[0]}
        return files

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

    def _get_error_for_bad_non_global_paths(
        self,
        qptiff_pair: tuple[list[Path], str],
        channels_paths: tuple[list[Path], str],
        base_path: Path,
    ) -> list[str]:
        errors = []
        for found_paths, filepath_glob in [qptiff_pair, channels_paths]:
            if len(found_paths) == 0:
                paths = [file for file in Path(base_path / "non_global").glob(filepath_glob)]
                if paths:
                    errors.append(
                        f"File(s) {', '.join([self.rel_filename_str(path) for path in paths])} found but missing from non_global_files column in metadata.tsv."
                    )
        return errors

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
    tmp_dir_base = Path("/tmp")

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)

    def _collect_errors(self):
        try:
            self._check_tmp_dir()
            super()._collect_errors()
        except Exception as e:
            self.errors.append(f"Error testing files: {e}")
        return self._return_result(self.errors, True)

    def _check_tmp_dir(self):
        self.tmp_dir = Path(self.tmp_dir_base / f"{self.uuid}_ome_xml")
        # may exist from previous run
        if not self.tmp_dir.exists():
            os.mkdir(self.tmp_dir)
        else:
            # note: any bad conversions will need to be removed manually
            print(f"Found existing temp directory {self.tmp_dir}")
        assert self.tmp_dir, f"Temp dir {self.tmp_dir} not created"

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
    def __init__(self):
        self.check_dependencies()

    def __call__(self, data_path: Path, file_dict: dict[str, Path], tmp_dir: Path) -> str | None:
        """
        Check that channels in channel_id column of qptiff.channels.csv
        match channels in accompanying OME-XML file (generated from QPTIFF).
        """
        self.tmp_dir = tmp_dir  # stays the same at the upload level
        try:
            csv_channels = self.get_csv_channels(file_dict["csv"])
            qptiff_channels = self.get_qptiff_channels(data_path, file_dict["qptiff"])
            if csv_channels.difference(qptiff_channels):
                return dedent(
                    f"""Channels in {get_rel_filename_str(data_path, file_dict["csv"])} don't match those in QPTIFF {get_rel_filename_str(data_path, file_dict["qptiff"])} (from converted OME-XML).

                        Channels in CSV not present in QPTIFF: {', '.join(csv_channels.difference(qptiff_channels))}

                        QPTIFF channels: {', '.join(qptiff_channels)}
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

    def extract_ome_xml(self, data_path: Path, qptiff_path: Path) -> Path:
        """
        The bioformats2raw params (except for no-tiles) are borrowed from the
        phenocycler pipeline (v1.4.8); major changes to how QPTIFFs are converted
        in the pipeline may desync this validation.
        """
        output_dirname = f"{data_path.stem}_{qptiff_path.stem}_converted"
        ome_xml_path = Path(self.tmp_dir / output_dirname / "OME/METADATA.ome.xml")
        if Path(self.tmp_dir / output_dirname).exists():
            if ome_xml_path.exists():
                print(f"Found existing OME-XML file {ome_xml_path}.")
                return ome_xml_path  # all good unless converted file is somehow incorrect
            else:
                raise Exception(
                    f"Curation: Output dir {output_dirname} exists but does not include OME-XML; it must be removed manually."
                )
        # note: files created by docker need to be removed from /tmp manually with sudo
        self.run_docker_bioformats2raw(qptiff_path, output_dirname)
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

    ##########
    # Docker #
    ##########

    image_name = f"bioformats2raw:{BIOFORMATS2RAW_RELEASE}"

    def check_dependencies(self):
        docker_images = (
            subprocess.check_output(
                f'docker image ls --filter "reference={self.image_name}"',
                stderr=subprocess.STDOUT,
                shell=True,
            )
            .decode("utf-8")
            .splitlines()
        )
        if len(docker_images) == 2:
            print(f"Found docker image {docker_images[1]}.")
        elif len(docker_images) > 2:
            # found header and more than one image result
            raise Exception(f"More than one '{self.image_name}': {docker_images}")
        elif len(docker_images) == 1:
            # only found header
            self.build_image()
        else:
            raise Exception(
                f"Error retrieving docker image {self.image_name}. Results: {docker_images}"
            )

    def build_image(self):
        docker_dir = Path(__file__).resolve().parent / "docker"
        if not docker_dir.exists() or not Path(docker_dir / "Dockerfile").exists():
            raise Exception(f"Missing Docker directory ({docker_dir}) or Dockerfile")
        cmd = ["docker", "build", f"--tag={self.image_name}", docker_dir]
        try:
            print(f"Building docker image {self.image_name}...")
            # print progress line by line
            build_process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            try:
                while build_process.poll() is None:
                    print(
                        build_process.stdout.readline()
                        if build_process.stdout
                        else "waiting for output..."
                    )
                print(build_process.stdout.read() if build_process.stdout else "")
            except Exception as e:
                print(e)
        except subprocess.CalledProcessError:
            raise Exception("Failed to create Docker image.")

    def run_docker_bioformats2raw(self, qptiff_path: Path, output_dirname: str):
        docker_input_mount = "/input"
        docker_output_mount = "/output"
        bioformats2raw_command = [
            "docker",
            "run",
            "--rm",  # automatically remove container when stopped
            "--mount",
            f"type=bind,src={qptiff_path.parent},dst={docker_input_mount},readonly",  # mount qptiff dir (readonly) as /input
            "--mount",
            f"type=bind,src={self.tmp_dir},dst={docker_output_mount}",  # mount tmp_dir (writeable) as /output
            self.image_name,
            "bioformats2raw/bin/bioformats2raw",
            "--resolutions",  # resolutions and series are mirrored from pipeline conversion
            "1",
            "--series",
            "0",
            "--no-tiles",  # do not convert image
            f"{docker_input_mount}/{qptiff_path.name}",  # input file
            f"{docker_output_mount}/{output_dirname}",  # output file
            "--memo-directory",  # silence warnings about memo file creation (auto-deleted)
            "/tmp",
        ]
        print(f"Running {bioformats2raw_command}")
        subprocess.check_call(bioformats2raw_command)
