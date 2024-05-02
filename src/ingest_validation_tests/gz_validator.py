import gzip
import re
from multiprocessing import Pool
from os import cpu_count
from typing import List

from ingest_validation_tools.plugin_validator import Validator


def _log(message: str):
    print(message)


class Engine(object):
    def __call__(self, filename):
        excluded = r".*/*fastq.gz"
        if re.search(excluded, filename.as_posix()):
            return
        try:
            _log(f"Threaded {filename}")
            with gzip.open(filename) as g_f:
                while True:
                    buf = g_f.read(1024 * 1024)
                    if not buf:
                        break
        except Exception as e:
            _log(f"{filename} is not a valid gzipped file {e}")
            return f"{filename} is not a valid gzipped file"


class GZValidator(Validator):
    description = "Recursively checking gzipped files for damage using multiprocessing pools"
    cost = 5.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[str]:
        data_output2 = []
        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        _log(f"Threading at {threads}")
        file_list = []
        for path in self.paths:
            for glob_expr in ["**/*.gz"]:
                file_list.extend(path.glob(glob_expr))
        try:
            pool = Pool(threads)
            engine = Engine()
            data_output = pool.imap_unordered(engine, file_list)
        except Exception as e:
            _log(f"Error {e}")
            pool.close()
            pool.join()
            data_output2.extend(f"Error: {e}")
        else:
            pool.close()
            pool.join()
            [data_output2.append(output) for output in data_output if output]
        return data_output2
