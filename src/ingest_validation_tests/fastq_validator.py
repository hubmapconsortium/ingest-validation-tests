import os
from typing import List

from ingest_validation_tools.plugin_validator import Validator
from fastq_validator_logic import FASTQValidatorLogic, _log


class FASTQValidator(Validator):
    description = "Check FASTQ files for basic syntax and consistency."
    cost = 15.0

    def collect_errors(self, **kwargs) -> List[str]:
        threads = kwargs.get('coreuse', None)
        if not threads:
            _log(f'No threads where sent for this plugin, defaulting to 25%')
            threads = os.cpu_count() // 4
        validator = FASTQValidatorLogic(verbose=True)
        validator.validate_fastq_files_in_path(self.path, threads)
        return validator.errors
