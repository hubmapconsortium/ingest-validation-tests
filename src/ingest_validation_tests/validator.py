import inspect
import os
import sys
from csv import DictReader
from enum import Enum
from importlib import util
from os import cpu_count
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import tifffile
import xmlschema

BASE_OME_XML_SCHEMA = Path(__file__).resolve().parent / "ome_tiff_schemas/2016-06_ome.xsd"


class Validator:
    description: str = "This is a human-readable description"
    """str: human-readable description of the thing this validator validates
    """

    cost: float = 1.0
    """float: a rough measure of cost to run.  Lower is better.
    """

    version: str = ""
    """string: in derived classes, a valid semantic version string.
    """

    required: list = []

    def __init__(
        self,
        base_paths: list[Path],
        assay_type: str,
        contains: list = [],
        verbose: bool = True,
        schema_rows: list = [],
        globus_token: str = "",
        app_context: dict[str, str] = {},
        coreuse: int | None = None,
        **kwargs,
    ):
        """
        Arguments:
            base_paths: list of directories (root paths of the directory trees to be validated)
            assay_type: assay type string to be checked against self.required and self.contains
            contains: information from upstream SchemaVersion, only provided by multi-assay uploads
            verbose: controls printing in self._log
            schema_rows: SchemaVersion.rows data from ingest-validation-tools
            globus_token: Globus auth token
            app_context: contains project and env-specific urls, headers
            coreuse: optionally pass in desired number of threads

        Usage:
            v = ValidatorSubclass(<base_paths>, <assay_type>, ...)
            errors = v.collect_errors()

        """
        if isinstance(base_paths, (Path, str)):
            self.paths = [Path(base_paths)]
        else:
            self.paths = [Path(path) for path in base_paths]
        self.assay_type = assay_type
        self.contains = contains
        self.verbose = verbose
        self.schema_rows = schema_rows
        if not self.schema_rows and (schema := kwargs.get("schema")):
            self.schema_rows = schema.rows
        self.token = globus_token
        self.app_context = app_context
        num_cpus = cpu_count()
        self.threads = coreuse if coreuse else num_cpus // 4 if (num_cpus and num_cpus >= 4) else 1
        self._log(f"Threading at {self.__class__.__name__} with {self.threads}")

    def collect_errors(self) -> list[str | None]:
        """
        Ensure plugin is valid, and if so, collect errors
        according to the subclass's _collect_errors method.
        """
        if not self.plugin_valid:
            return []
        self._log(f"Update: threading at {self.__class__.__name__} with {self.threads}")
        return self._collect_errors()

    @property
    def plugin_valid(self) -> bool:
        self._log(f"Required assay_type: {self.required}")
        if not self.required:
            # Plugin runs for all dataset_types
            return True
        if self.assay_type.lower() in self.required:
            return True
        elif set(self.required).intersection(set(self.contains)):
            return True
        self._log("Plugin not relevant; did not run.")
        return False

    def _collect_errors(self) -> list[str | None]:
        raise NotImplementedError()

    def _return_result(self, rslt_list: list | None, data_tested: list | bool) -> list[str | None]:
        """
        Return the errors found by this validator.

        Arguments:
            rslt_list: list of errors found by plugin
            data_tested: list of (usually) files tested by plugin or bool
                representing whether data was tested

        Returns:
            list[str]: Truthy rslt_list, return list of human-readable error messages
            list[None]: Falsey rslt_list but truthy data_tested, report plugin run
            list[]: neither rslt_list nor data_tested, report plugin not run
        """
        if rslt_list:
            self._log("Errors found.")
            return rslt_list
        elif data_tested:
            self._log("No errors found.")
            return [None]
        self._log("Plugin not relevant. Not run.")
        return []

    def _log(self, message):
        if self.verbose:
            print(message)
            return message

    def rel_filename_str(self, filename: Path) -> str:
        return get_rel_filename_str(self.paths[0], filename)

    @property
    def uuid(self) -> str:
        for elt in reversed(str(self.paths[0]).split(os.sep)):
            if len(elt) == 32 and all([c in "0123456789abcdef" for c in list(elt)]):
                return elt
        raise Exception("no uuid was found in the path to the current working directory")


##############
# Find files #
##############


class FileTypes(Enum):
    QPTIFF = "raw/images"
    OME_TIFF = "lab_processed/images"


def find_files(
    data_path: Path, file_type: FileTypes, restrict_to_expected: bool = False
) -> list[Path]:
    """
    Search for files in expected_dir; if expected_dir does not exist
    or if no files found, search entire data_path.

    Arguments:
        data_path: base path to search
        file_type: file type to search for
        restrict_to_expected: do not look outside of expected_dir
    """
    valid_files = []
    expected_dir = Path(data_path / file_type.value)
    if expected_dir.exists():
        for filepath in expected_dir.iterdir():
            if verify_filename(filepath, file_type):
                valid_files.append(filepath)
    if not valid_files and not restrict_to_expected:
        valid_files = find_all_files(data_path, file_type)
    return valid_files


def find_all_files(data_path: Path, file_type: FileTypes) -> list[Path]:
    """
    Recursively search entire data_path for matching files.
    """
    valid_files = []
    for path, _, files in os.walk(data_path):
        valid_files.extend(
            [
                Path(path, filename)
                for filename in files
                if verify_filename(Path(path, filename), file_type)
            ]
        )
    return valid_files


def verify_filename(filepath: Path | str, file_type: FileTypes) -> bool:
    if file_type == FileTypes.QPTIFF:
        return verify_qptiff_filename(filepath)
    elif file_type == FileTypes.OME_TIFF:
        return verify_ome_tiff_filename(filepath)


def verify_qptiff_filename(file: str | Path) -> bool:
    path = Path(str(file).lower())
    try:
        assert path.suffix == ".qptiff"
        assert ".raw" not in path.stem
        assert ".intermediate" not in path.stem
        assert "extras" not in path.parts
    except AssertionError:
        return False
    return True


def verify_ome_tiff_filename(file: str | Path) -> bool:
    path = Path(str(file).lower())
    try:
        assert path.suffix in [".tiff", ".tif"]
        assert ".ome.tif" in path.name
        assert ".raw" not in path.stem
        assert ".intermediate" not in path.stem
        assert "extras" not in path.parts
    except AssertionError:
        return False
    return True


#########
# Utils #
#########


def get_non_global_paths_by_row(rows: list[dict], base_path: Path) -> dict[int, list[Path]]:
    """
    Create dict of non-global paths by row for a shared upload.
    {<row_index_0>: [<path_1>, <path_2>], <row_index_1>: [<path_3>, <path_4>]}
    Return only if all paths exist, else raise.
    """
    files_by_row = {}
    errors = []
    for i, row in enumerate(rows):
        files = []
        non_global_files = row.get("non_global_files", "")
        filepaths = [
            Path(base_path / f"non_global/{file.strip()}") for file in non_global_files.split(";")
        ]
        for file in filepaths:
            if not file.exists():
                errors.append(get_rel_filename_str(base_path, file))
            else:
                files.append(file)
        files_by_row[i] = files
    if errors:
        raise Exception(
            f"Files listed in non_global_files field do not exist: {', '.join(errors)}"
        )
    return files_by_row


def read_tsv(path: Path, encoding: str = "utf-8") -> list[dict]:
    with open(path, encoding=encoding) as f:
        rows = list(DictReader(f, dialect="excel-tab"))
        f.close()
    return rows


def check_ome_tiff_file(file: str | Path) -> xmlschema.XmlDocument:
    """
    OME-TIFF -> validated XmlDocument
    """
    extracted_xml = extract_ome_xml(file)
    return check_ome_xml(extracted_xml, file)


def extract_ome_xml(file: str | Path) -> str:
    """
    OME-TIFF -> OME-XML string
    """
    with tifffile.TiffFile(file) as tf:
        if not (extracted_xml := tf.ome_metadata):
            raise Exception(f"{file} is not a valid OME.TIFF file: No XML found in OME.TIFF file.")
    return extracted_xml


def check_ome_xml(xml_string: str, file: str | Path) -> xmlschema.XmlDocument:
    """
    OME-XML string -> validated XmlDocument
    """
    try:
        ET.fromstring(xml_string)  # yields more descriptive parsing error
        xml_document = xmlschema.XmlDocument(xml_string, schema=BASE_OME_XML_SCHEMA)  # type: ignore
        if xml_document.schema and not xml_document.schema.is_valid(xml_document):
            raise Exception("Schema not valid.")
        elif not xml_document.schema:
            raise Exception("Can't read OME XML.")
    except (xmlschema.exceptions.XMLResourceParseError, ET.ParseError) as excp:
        raise Exception(f"Error parsing {file}: {excp}")
    except Exception as excp:
        print(f"{file} is not a valid OME.TIFF file: {excp}")
        raise Exception(f"{file} is not a valid OME.TIFF file: {excp}")
    return xml_document


def convert_to_micrometers(value: float | int, unit: str) -> float:
    """
    Very minimal set of options for converting unit measurements
    valid in OME-XML "PhysicalSizeUnit" field.
    """
    match unit:
        case "mm":
            return value * 1000
        case "µm" | "um":
            return value
        case "nm":
            return value / 1000
        case _:
            raise Exception(f"Unit '{unit}' is not supported.")


#######
# API #
#######


def validation_class_iter() -> list[Validator]:
    """
    Return the validator types in order of increasing cost.
    """
    plugins = list(Path(__file__).parent.glob("*.py"))
    sort_me = []
    for fpath in plugins:
        mod_nm = fpath.stem
        if mod_nm in sys.modules:
            mod = sys.modules[mod_nm]
        else:
            spec = util.spec_from_file_location(mod_nm, fpath)
            if spec is None:
                raise Exception(f"bad plugin test {fpath}")
            mod = util.module_from_spec(spec)
            sys.modules[mod_nm] = mod
            if spec.loader:
                spec.loader.exec_module(mod)
            else:
                raise Exception(f"bad plugin test {fpath}; no loader found")
        for _, obj in inspect.getmembers(mod):
            if inspect.isclass(obj) and obj != Validator and issubclass(obj, Validator):
                sort_me.append((obj.cost, obj.description, obj))
    sort_me.sort()
    sorted_classes = []
    for _, _, val_class in sort_me:
        sorted_classes.append(val_class)
    return sorted_classes


def get_rel_filename_str(comparison_path: Path | int, filename: Path) -> str:
    """
    In the case of shared uploads, comparison_path may be an int (row number).
    """
    if isinstance(comparison_path, int):
        return str(filename)
    try:
        return str(filename.relative_to(comparison_path.parent))
    except Exception:
        return str(filename)


def csv_to_df(path: Path, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        raise Exception(f"Unexpected error reading {str(path)}: {e}")
