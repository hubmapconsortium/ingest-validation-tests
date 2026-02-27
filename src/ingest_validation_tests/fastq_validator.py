from fastq_validator_logic import FASTQValidatorLogic
from validator import Validator


class FASTQValidator(Validator):
    description = "Check FASTQ files for basic syntax and consistency."
    cost = 15.0
    version = "1.0"

    def _collect_errors(self) -> list[str | None]:
        validator = FASTQValidatorLogic(verbose=True)
        validator.validate_fastq_files_in_path(self.paths, self.threads)
        return self._return_result(validator.errors, validator.files_were_found)
