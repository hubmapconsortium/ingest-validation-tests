from multiprocessing import Pool
from typing import List

import gzip
from ingest_validation_tools.plugin_validator import Validator


def _log(message: str):
    print(message)


class Engine(object):
    def __call__(self, filename):
        try:
            _log(f'Threaded {filename}')
            with gzip.open(filename) as g_f:
                while True:
                    buf = g_f.read(1024*1024)
                    if not buf:
                        break
        except Exception as e:
            _log(f'{filename} is not a valid gzipped file {e}')
            return f'{filename} is not a valid gzipped file'


class GZValidator(Validator):
    description = "Recursively checking gzipped files for damage using multiprocessing pools"
    cost = 5.0

    def collect_errors(self, **kwargs) -> List[str]:
        data_output2 = []
        for glob_expr in ['**/*.gz']:
            try:
                pool = Pool(16)
                engine = Engine()
                data_output = pool.map(engine, self.path.glob(glob_expr))
            except Exception as e:
                _log(f'Error {e}')
            else:
                pool.close()
                pool.join()
                [data_output2.append(output) for output in data_output if output]
        return data_output2

