from pathlib import Path
from typing import List, Generator

from ingest_validation_tools.plugin_validator import Validator

_FASTQ_FILE_MATCH = '**/*.fastq*'


def has_valid_name(filename: str) -> bool:
    return filename.endswith('.fastq.gz') or filename.endswith('.fastq')


class FASTQValidator(Validator):
    """Validate FASTQ input files for basic syntax."""

    def collect_errors(self, **kwargs) -> List[str]:
        self.errors: List[str] = []
        found_one = False

        fastq_file: Path
        for fastq_file in self.path.glob(_FASTQ_FILE_MATCH):
            found_one = True

            if not has_valid_name(fastq_file.name):
                self.errors.append(
                    f"{fastq_file} is not an expected filename and will "
                    "not be processed."
                )

        if not found_one:
            self.errors.append(f"No files matching {_FASTQ_FILE_MATCH} were "
                               f"found in {self.path}.")

        return self.errors
