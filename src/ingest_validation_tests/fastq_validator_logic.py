import gzip
import re

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, TextIO


def _validate_fastq_filename(filename: str) -> List[str]:
    valid_filename = filename.endswith('.fastq.gz') or \
                     filename.endswith('.fastq')
    if not valid_filename:
        return ["Filename does not have proper format and will not be "
                "processed"]

    return []


def _open_fastq_file(file: Path) -> TextIO:
    return (
        gzip.open(file, 'rt') if file.name.endswith('.gz')
        else file.open()
    )


class FASTQValidatorLogic:
    """Validate FASTQ input files for basic syntax.

    Syntax validated is based on what is listed on Wikipedia
    https://en.wikipedia.org/wiki/FASTQ_format.

    Line 1 begins with a '@' character and is followed by a sequence identifier
    and an optional description (like a FASTA title line).
    Line 2 is the raw sequence letters.
    Line 3 begins with a '+' character and is optionally followed by the same
    sequence identifier (and any description) again.
    Line 4 encodes the quality values for the sequence in Line 2, and must
    contain the same number of symbols as letters in the sequence.
    """

    _FASTQ_FILE_MATCH = '**/*.fastq*'

    # This pattern seeks out the string that includes the lane number (since that
    # is expected to be present to help anchor the prefix) that comes before any of
    # _I1, _I2, _R1, or _R2.
    _FASTQ_FILE_PREFIX_REGEX = re.compile(r'(.+_L\d+.*)_[IR][12][._]')

    _FASTQ_LINE_2_VALID_CHARS = 'ACGNT'

    def __init__(self):
        self.errors: [str] = []
        self._filename = ''
        self._line_number = 0
        self._file_record_counts: Dict[str, int] = {}
        self._file_prefix_counts: Dict[str, int] = {}

        self._line_2_length = 0

    def _format_error(self, error: str) -> str:
        location = self._filename
        if self._line_number:
            location += f":{self._line_number}"

        return f"{location}: {error}"

    def _validate_fastq_line_1(self, line: str) -> List[str]:
        if not line or line[0] != '@':
            return ["Line does not begin with '@'."]

        return []

    def _validate_fastq_line_2(self, line: str) -> List[str]:
        self._line_2_length = len(line)

        invalid_chars = ''.join(
            c for c in line if c not in self._FASTQ_LINE_2_VALID_CHARS)
        if invalid_chars:
            return [f"Line contains invalid character(s): {invalid_chars}"]

        return []

    def _validate_fastq_line_3(self, line: str) -> List[str]:
        if not line or line[0] != '+':
            return ["Line does not begin with '+'."]

        return []

    def _validate_fastq_line_4(self, line: str) -> List[str]:
        errors: [str] = []

        invalid_chars = ''.join(c for c in line if not 33 <= ord(c) <= 126)
        if invalid_chars:
            errors.append("Line contains invalid quality character(s): "
                          f'"{invalid_chars}"')

        if len(line) != self._line_2_length:
            errors.append(f"Line contains {len(line)} characters which "
                          "does not match line 2's "
                          f"{self._line_2_length} characters.")

        return errors

    _VALIDATE_FASTQ_LINE_METHODS = {1: _validate_fastq_line_1,
                                    2: _validate_fastq_line_2,
                                    3: _validate_fastq_line_3,
                                    4: _validate_fastq_line_4}

    def validate_fastq_record(self, line: str, line_number: int) -> List[str]:
        line_index = line_number % 4 + 1

        validator_method: Callable[[FASTQValidatorLogic, str], List] = \
            self._VALIDATE_FASTQ_LINE_METHODS[line_index]

        assert validator_method, \
            f"No validator method defined for record index {line_index}"

        return validator_method(self, line)

    @dataclass
    class ValidationResult:
        record_count = 0
        errors: List[str] = field(default_factory=list)

    def validate_fastq_stream(self, fastq_data: TextIO) -> ValidationResult:
        line_count = 0

        result = self.ValidationResult()

        line: str
        for line_count, line in enumerate(fastq_data):
            self._line_number = line_count + 1
            result.errors.extend(
                self._format_error(error) for error in
                self.validate_fastq_record(line.rstrip(), line_count)
            )

        result.record_count = line_count + 1
        return result

    def validate_fastq_file(self, fastq_file: Path) -> List[str]:
        filename_errors = _validate_fastq_filename(fastq_file.name)
        if filename_errors:
            # If we don't like the filename, don't bother reading the contents.
            return filename_errors

        try:
            with _open_fastq_file(fastq_file) as fastq_data:
                result = self.validate_fastq_stream(fastq_data)
        except gzip.BadGzipFile:
            return [self._format_error("FASTQ .gz file cannot be read")]
        except IOError:
            return [self._format_error("Unable to open FASTQ data file.")]

        if fastq_file.name in self._file_record_counts.keys():
            result.errors.append(f"{fastq_file.name} has been found multiple "
                                 "times during this validation.")
        self._file_record_counts[fastq_file.name] = result.record_count

        match = self._FASTQ_FILE_PREFIX_REGEX.match(fastq_file.name)
        if match:
            filename_prefix = match.group(1)
            if filename_prefix in self._file_prefix_counts.keys():
                extant_count = self._file_prefix_counts[filename_prefix]
                if extant_count != result.record_count:
                    # Find a file we've validated already that matches this
                    # prefix.
                    extant_files = [
                        filename for filename
                        in self._file_record_counts.keys()
                        if self._file_record_counts[filename] == extant_count
                           and filename.startswith(filename_prefix)
                    ]
                    # Based on how the dictionaries are created, there should
                    # always be at least one matching filename.
                    assert extant_files

                    result.errors.append(
                        f"{fastq_file.name} ({result.record_count} lines) "
                        f"does not match length of {extant_files[0]} "
                        f"({extant_count} lines).")
            else:
                self._file_prefix_counts[filename_prefix] = result.record_count

        return result.errors

    def validate_fastq_files_in_path(self, path: Path) -> List[str]:
        found_one = False
        errors: [str] = []

        fastq_file: Path
        for fastq_file in path.glob(self._FASTQ_FILE_MATCH):
            self._filename = fastq_file.name

            new_errors = self.validate_fastq_file(fastq_file)
            if new_errors:
                errors.extend(new_errors)
            else:
                # If we successfully process any input file, this can be set.
                found_one = True

        if not found_one:
            errors.append(f"No good files matching {self._FASTQ_FILE_MATCH} "
                          f"were found in {path}.")

        return errors
