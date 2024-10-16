from typing import List, Optional

from fastq_validator_logic import FASTQValidatorLogic, _log
from ingest_validation_tools.plugin_validator import Validator


class FASTQValidator(Validator):
    description = "Check FASTQ files for basic syntax and consistency."
    cost = 15.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        del kwargs
        _log(f"Threading at FastQValidator with {self.thread_count}")
        validator = FASTQValidatorLogic(verbose=True)
        validator.validate_fastq_files_in_path(self.paths, self.thread_count)
        if validator.errors:
            return validator.errors
        elif validator.files_were_found:
            return [None]
        else:
            return []
