from os import cpu_count
from multiprocessing import Pool
from typing import List

import tifffile
from ingest_validation_tools.plugin_validator import Validator

def _log(message: str):
    print(message)


def _check_tiff_file(path: str) -> str or None:
    try:
        with tifffile.TiffFile(path) as tfile:
            for page in tfile.pages:
                _ = page.shape
                _ = page.dtype
            return None
    except Exception as excp:
        _log(f"{path} is not a valid TIFF file: {excp}")
        return f"{path} is not a valid TIFF file"


class TiffValidator(Validator):
    description = "Recursively test all tiff files that are not ome.tiffs for validity"
    cost = 1.0
    def collect_errors(self, **kwargs) -> List[str]:
        threads = kwargs.get('coreuse', None) or cpu_count() // 4 or 1
        print(f"THREADS {threads}")
        pool = Pool(threads)
        filenames_to_test = []
        for glob_expr in ['**/*.tif', '**/*.tiff', '**/*.TIFF', '**/*.TIF']:
            for path in self.path.glob(glob_expr):
                if not path.name.lower().endswith(('.ome.tif', '.ome.tiff')):
                    filenames_to_test.append(path)
        return list(rslt for rslt in pool.imap_unordered(_check_tiff_file,
                                                         filenames_to_test)
                    if rslt is not None)
