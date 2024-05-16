from os import cpu_count
from typing import List

from fastq_validator_logic import FASTQValidatorLogic, _log
from ingest_validation_tools.plugin_validator import Validator


class FASTQValidator(Validator):
    description = "Check FASTQ files for basic syntax and consistency."
    cost = 15.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[str]:
        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        _log(f"Threading at {threads}")
        validator = FASTQValidatorLogic(verbose=True)
        validator.validate_fastq_files_in_path(self.paths, threads)
        if validator.errors:
            return validator.errors
        elif validator.files_were_found:
            return [None]
        else:
            return []
