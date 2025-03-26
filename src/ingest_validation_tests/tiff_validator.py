import logging
from multiprocessing import Pool
from os import cpu_count
from typing import List, Optional

import tifffile
from ingest_validation_tools.plugin_validator import Validator


def filter(record):
    if record.levelno >= logging.WARNING:
        raise ValueError(record)
    return True


def _log(message: str):
    print(message)


def _check_tiff_file(path: str) -> Optional[str]:
    tifffile.logger().addFilter(filter)
    try:
        with tifffile.TiffFile(path) as tfile:
            for page in tfile.pages:
                _ = page.asarray()  # force decompression
        tifffile.logger().removeFilter(filter)
        return None
    except Exception as excp:
        _log(f"{path} is not a valid TIFF file: {excp}")
        return f"{path} is not a valid TIFF file: {excp}"


class TiffValidator(Validator):
    description = "Recursively test all tiff files that are not ome.tiffs for validity"
    cost = 1.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        _log(f"Threading at TiffValidator with {threads}")
        pool = Pool(threads)
        filenames_to_test = []
        for glob_expr in ["**/*.tif", "**/*.tiff", "**/*.TIFF", "**/*.TIF"]:
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
