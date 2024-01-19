from os import cpu_count
from typing import List

from fastq_validator_logic import FASTQValidatorLogic, _log
from ingest_validation_tools.plugin_validator import Validator


class FASTQValidator(Validator):
    description = "Check FASTQ files for basic syntax and consistency."
    cost = 15.0

    def collect_errors(self, **kwargs) -> List[str]:
        _log(f'Threading at {self.threads}')
        validator = FASTQValidatorLogic(verbose=True)
        validator.validate_fastq_files_in_path(self.paths, self.pool)
        return validator.errors
