import re
from typing import List

import gzip
from ingest_validation_tools.plugin_validator import Validator


def _log(message: str):
    print(message)


class Engine(object):
    def __call__(self, path_list):
        for path in path_list:
            for filename in path.glob('**/*.gz'):
                excluded = r'.*/*fastq.gz'
                if re.search(excluded, filename.as_posix()):
                    return
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
        _log(f'Threading at {self.threads}')
        try:
            engine = Engine()
            data_output = self.pool.imap_unordered(engine, self.paths)
        except Exception as e:
            _log(f'Error {e}')
        else:
            self.pool.close()
            self.pool.join()
            [data_output2.append(output) for output in data_output if output]
        return data_output2
