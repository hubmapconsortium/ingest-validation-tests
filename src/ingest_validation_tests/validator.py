import inspect
import sys
from importlib import util
from pathlib import Path
from typing import Union


class Validator(object):
    description = "This is a human-readable description"
    """str: human-readable description of the thing this validator validates
    """

    cost = 1.0
    """float: a rough measure of cost to run.  Lower is better.
    """

    version = ""
    """string: in derived classes, a valid semantic version string.
    """

    def __init__(
        self,
        base_paths: list[Path],
        assay_type: str,
        contains: list = [],
        verbose: bool = False,
        schema=None,  # type is Optional[SchemaVersion] but not worth importing SchemaVersion from IVTools
        globus_token: str = "",
        app_context: dict[str, str] = {},
        **kwargs,
    ):
        """
        base_paths is expected to be a list of directories.
        These are the root paths of the directory trees to be validated.
        assay_type is an assay type, one of a known set of strings.
        """
        del kwargs
        if isinstance(base_paths, list):
            self.paths = [Path(path) for path in base_paths]
        elif isinstance(base_paths, (Path, str)):
            self.paths = [Path(base_paths)]
        else:
            raise Exception(f"Validator init received base_paths arg as type {type(base_paths)}")
        self.assay_type = assay_type
        self.contains = contains
        self.verbose = verbose
        self.schema = schema
        self.token = globus_token
        self.app_context = app_context

    def _log(self, message):
        if self.verbose:
            print(message)
            return message

    def collect_errors(self, **kwargs) -> list[Union[str, None]]:
        """
        Returns a list of strings, each of which is a
        human-readable error message.

        "No error" is represented by returning an empty list.
        If the assay_type is not one for which this validator is intended,
        just return an empty list.
        """
        del kwargs
        raise NotImplementedError()


PathOrStr = Union[str, Path]


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
