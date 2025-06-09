from multiprocessing import Pool
from os import cpu_count
from typing import List, Optional

import tifffile
from ingest_validation_tools.plugin_validator import Validator

# monkey patch tifffile to raise an exception every time a warning
# is logged
original_log_warning = tifffile.tifffile.log_warning


def my_log_warning(msg, *args, **kwargs):
    raise RuntimeError(f"{msg.format(*args, **kwargs)}")


tifffile.tifffile.log_warning = my_log_warning


def _log(message: str):
    print(message)


def _check_tiff_file(path: str) -> Optional[str]:
    try:
        with tifffile.TiffFile(path) as tfile:
            for page in tfile.pages:
                _ = page.asarray()  # force decompression
        return None
    except Exception as excp:
        _log(f"{path} is not a valid TIFF file: {excp}")
        return f"{path} is not a valid TIFF file: {excp}"


class TiffValidator(Validator):
    description = "Recursively test all tiff files (including ome.tiffs) for validity"
    cost = 1.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        _log(f"Threading at TiffValidator with {threads}")
        pool = Pool(threads)
        filenames_to_test = []
        for glob_expr in [
            "**/*.[tT][iI][fF]",
            "**/*.[tT][iI][fF][fF]",
        ]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)
        try:
            rslt_list: List[Optional[str]] = list(
                rslt
                for rslt in pool.imap_unordered(_check_tiff_file, filenames_to_test)
                if rslt is not None
            )
        except Exception as e:
            _log(f"Error {e}")
            rslt_list = [f"Error {e}"]
        pool.close()
        pool.join()
        if rslt_list:
            return rslt_list
        elif filenames_to_test:
            return [None]
        else:
            return []
