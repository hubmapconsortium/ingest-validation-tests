import argparse
import gzip
import re

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


def _log(message: str) -> str:
    print(message)
    return message


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

    # This pattern seeks out the string that includes the lane number (since
    # that is expected to be present to help anchor the prefix) that comes
    # before any of _I1, _I2, _R1, or _R2.
    _FASTQ_FILE_PREFIX_REGEX = re.compile(r'(.+_L\d+.*)_[IR][12][._]')

    _FASTQ_LINE_2_VALID_CHARS = 'ACGNT'

    def __init__(self, verbose=False):
        self.errors: [str] = []
        self._filename = ''
        self._line_number = 0
        self._file_record_counts: Dict[str, int] = {}
        self._file_prefix_counts: Dict[str, int] = {}

        self._verbose = verbose

        self._line_2_length = 0
        self._last_line_2_number = 0

    def _format_error(self, error: str) -> str:
        location = self._filename
        if self._line_number:
            location += f":{self._line_number}"

        message = f"{location}: {error}"

        if self._verbose:
            _log(message)
        return message

    def _validate_fastq_line_1(self, line: str) -> List[str]:
        if not line or line[0] != '@':
            return ["Line does not begin with '@'."]

        return []

    def _validate_fastq_line_2(self, line: str) -> List[str]:
        self._line_2_length = len(line)
        self._last_line_2_number = self._line_number

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
                          f"does not match line {self._last_line_2_number}'s "
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

    class ValidationResult:
        record_count = 0
        errors: List[str]

        def __init__(self):
            self.errors = list()

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
        _log(f"Validating {fastq_file.name}...")
        filename_errors = _validate_fastq_filename(fastq_file.name)
        if filename_errors:
            # If we don't like the filename, don't bother reading the contents.
            return filename_errors

        self._line_number = 0
        self._filename = fastq_file.name

        try:
            with _open_fastq_file(fastq_file) as fastq_data:
                result = self.validate_fastq_stream(fastq_data)
        # TODO: Add gzip.BadGzipFile when Python 3.8 is available
        except IOError:
            return [self._format_error("Unable to open FASTQ data file.")]

        if fastq_file.name in self._file_record_counts.keys():
            result.errors.append(_log(
                f"{fastq_file.name} has been found multiple times during this "
                "validation."))
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
                        filename for filename, record_count
                        in self._file_record_counts.items()
                        if record_count == extant_count
                            and filename.startswith(filename_prefix)
                    ]
                    # Based on how the dictionaries are created, there should
                    # always be at least one matching filename.
                    assert extant_files

                    result.errors.append(_log(
                        f"{fastq_file.name} ({result.record_count} lines) "
                        f"does not match length of {extant_files[0]} "
                        f"({extant_count} lines)."))
            else:
                self._file_prefix_counts[filename_prefix] = result.record_count

        return result.errors

    def validate_fastq_files_in_path(self, path: Path) -> List[str]:
        found_one = False
        errors: [str] = []

        _log(f"Validating matching files in {path.as_posix()}")

        fastq_file: Path
        for fastq_file in path.glob(self._FASTQ_FILE_MATCH):
            new_errors = self.validate_fastq_file(fastq_file)
            if new_errors:
                errors.extend(new_errors)
            else:
                # If we successfully process any input file, this can be set.
                found_one = True

        if not found_one:
            errors.append(_log(
                f"No good files matching {self._FASTQ_FILE_MATCH} were found "
                f"in {path}."))

        return errors


def main():
    parser = argparse.ArgumentParser(description='Validate FASTQ files.')
    parser.add_argument('filepaths', type=Path, nargs='+',
                        help="Files to validate for FASTQ syntax")

    args = parser.parse_args()

    validator = FASTQValidatorLogic(True)

    path: Path
    for path in args.filepaths:
        if path.is_dir():
            validator.validate_fastq_files_in_path(path)
        else:
            validator.validate_fastq_file(path)


if __name__ == '__main__':
    main()
