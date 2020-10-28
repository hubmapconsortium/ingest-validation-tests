from typing import List

import gzip
from zlib import error as ZlibError
from ingest_validation_tools.plugin_validator import Validator

class GZValidator(Validator):
    description = "Recursively checking gzipped files for damage"
    cost = 5.0
    def collect_errors(self) -> List[str]:
        rslt = []
        for glob_expr in ['**/*.gz']:
            for path in self.path.glob(glob_expr):
                try:
                    with gzip.open(path) as g_f:
                        while True:
                            buf = g_f.read(1024*1024)
                            if not buf:
                                break
                except Exception:
                    rslt.append(f'{path} is not a valid gzipped file')
        return rslt


