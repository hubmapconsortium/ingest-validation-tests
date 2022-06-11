from typing import List

from ingest_validation_tools.plugin_validator import Validator
from fastq_validator_logic import FASTQValidatorLogic


class FASTQValidator(Validator):
    description = "Check FASTQ files for basic syntax and consistency."
    cost = 5.0

    def collect_errors(self, **kwargs) -> List[str]:
        validator = FASTQValidatorLogic(verbose=True)
        validator.validate_fastq_files_in_path(self.path)
        return validator.errors
