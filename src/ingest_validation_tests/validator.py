import inspect
import sys
from importlib import util
from os import cpu_count
from pathlib import Path

import tifffile
import xmlschema


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
        schema=None,
        globus_token: str = "",
        app_context: dict[str, str] = {},
        coreuse: int | None = None,
    ):
        """
        Arguments:
            base_paths: list of directories (root paths of the directory trees to be validated)
            assay_type: assay type string to be checked against self.required and self.contains
            contains: information from upstream SchemaVersion, only provided by multi-assay uploads
            verbose: controls printing in self._log
            schema: SchemaVersion object from ingest-validation-tools
            globus_token: Globus auth token
            app_context: contains project and env-specific urls, headers
            coreuse: optionally pass in desired number of threads

        Usage:
            v = ValidatorSubclass(<base_paths>, <assay_type>, ...)
            errors = v.collect_errors()

        """
        if isinstance(base_paths, list):
            self.paths = [Path(path) for path in base_paths]
        elif isinstance(base_paths, (Path, str)):
            self.paths = [Path(base_paths)]
        else:
            # No plugin will run, halt validation
            raise Exception(f"Validator init received base_paths arg as type {type(base_paths)}")
        self.assay_type = assay_type
        self.contains = contains
        self.verbose = verbose
        self.schema = schema
        self.token = globus_token
        self.app_context = app_context
        num_cpus = cpu_count()
        self.threads = coreuse if coreuse else num_cpus // 4 if (num_cpus and num_cpus >= 4) else 1
        self._log(f"Threading at {self.__class__.__name__} with {self.threads}")

    def collect_errors(self, **kwargs) -> list[str | None]:
        """
        Ensure plugin is valid, and if so, collect errors
        according to the subclass's _collect_errors method.
        """
        if not self.plugin_valid:
            return []
        # kwargs and coreuse included here for backward compatibility.
        if coreuse := kwargs.get("coreuse"):
            self.threads = coreuse
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


def check_ome_tiff_file(file: str | Path) -> xmlschema.XmlDocument:
    try:
        with tifffile.TiffFile(file) as tf:
            xml_document = xmlschema.XmlDocument(tf.ome_metadata)  # type: ignore
            if xml_document.schema and not xml_document.schema.is_valid(xml_document):
                raise Exception(f"{file} is not a valid OME.TIFF file: schema not valid")
            elif not xml_document.schema:
                raise Exception(f"Can't read OME XML from file {file}.")
    except Exception as excp:
        print(f"{file} is not a valid OME.TIFF file: {excp}")
        raise Exception(f"{file} is not a valid OME.TIFF file: {excp}")
    return xml_document


ome_tiff_globs = [
    "**/*.[oO][mM][eE].[tT][iI][fF]",
    "**/*.[oO][mM][eE].[tT][iI][fF][fF]",
]


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
