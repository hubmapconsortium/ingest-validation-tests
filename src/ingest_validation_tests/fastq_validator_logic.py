import argparse
import gzip
import logging
import re
from collections import defaultdict
from multiprocessing import Lock, Manager, Pool
from pathlib import Path
from typing import Callable, List, TextIO

import fastq_utils


def is_valid_filename(filename: str) -> bool:
    return bool(fastq_utils.FASTQ_PATTERN.fullmatch(filename))


def _open_fastq_file(file: Path) -> TextIO:
    return (
        gzip.open(file, 'rt') if file.name.endswith('.gz')
        else file.open()
    )


def _log(message: str) -> str:
    print(message)
    return message


class Engine(object):
    def __init__(self, validate_object):
        self.validate_object = validate_object

    def __call__(self, fastq_file):
        errors = []
        _log(f"Validating matching fastq file {fastq_file}")
        self.validate_object.validate_fastq_file(fastq_file)
        for err in self.validate_object.errors:
            errors.append(err)
        return errors


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

    _FASTQ_LINE_2_VALID_CHARS = 'ACGNT'

    def __init__(self, verbose=False):
        self.errors: List[str] = []
        self._file_record_counts = Manager().dict()
        self._file_prefix_counts = Manager().dict()
        self._filename = ''
        self._line_number = 0

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
        errors: List[str] = []
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

        validator_method: Callable[[FASTQValidatorLogic, str], List[str]] = \
            self._VALIDATE_FASTQ_LINE_METHODS[line_index]

        assert validator_method, \
            f"No validator method defined for record index {line_index}"

        return validator_method(self, line)

    def validate_fastq_stream(self, fastq_data: TextIO) -> int:
        # Returns the number of records read from fastq_data.
        line_count = 0

        line: str
        for line_count, line in enumerate(fastq_data):
            self._line_number = line_count + 1
            self.errors.extend(
                self._format_error(error) for error in
                self.validate_fastq_record(line.rstrip(), line_count)
            )

        return line_count + 1

    def validate_fastq_file(self, fastq_file: Path) -> None:
        _log(f"Validating {fastq_file.name}...")
        _log(f"    → {fastq_file.absolute().as_posix()}")

        if not is_valid_filename(fastq_file.name):
            # If we don't like the filename, don't bother reading the contents.
            self.errors.append(_log(
                "Filename does not have proper format "
                "and will not be processed"))
            return

        self._line_number = 0
        self._filename = fastq_file.name

        try:
            with _open_fastq_file(fastq_file) as fastq_data:
                records_read = self.validate_fastq_stream(fastq_data)
        except gzip.BadGzipFile:
            self.errors.append(
                self._format_error(f"Bad gzip file: {fastq_file}."))
            return
        except IOError:
            self.errors.append(
                self._format_error(f"Unable to open FASTQ data file {fastq_file}."))
            return
        self._file_record_counts[str(fastq_file)] = records_read

    def validate_fastq_files_in_path(self, paths: List[Path], threads: int) -> None:
        data_found_one = []
        dirs_and_files = defaultdict(dict)
        for path in paths:
            dirs_and_files[path] = fastq_utils.collect_fastq_files_by_directory(path)
            _log(f"Added files from {path} to dirs_and_files: {dirs_and_files}")
        file_list = []
        with Manager() as manager:
            lock = manager.Lock()
            for path, rel_paths in dirs_and_files.items():
                for rel_path, files in rel_paths.items():
                    for file in files:
                        file_list.append(Path(path / rel_path / file))
            try:
                logging.info(f"Passing file list for paths {paths} to engine. File list: {file_list}.")
                pool = Pool(threads)
                engine = Engine(self)
                data_output = pool.imap_unordered(engine, file_list)
            except Exception as e:
                _log(f"Error {e}")
            else:
                pool.close()
                pool.join()
                self._find_duplicates(dirs_and_files)
                self._find_shared_prefixes(lock)
                [data_found_one.extend(output) for output in data_output if output]

        if len(data_found_one) > 0:
            self.errors.extend(data_found_one)

    def _find_duplicates(self, dirs_and_files):
        for data_path, sub_dirs in dirs_and_files.items():
            # Creates a dict of filenames to list of full filepaths for each
            # fastq file in a given data_path (dataset dir).
            files_per_path = defaultdict(list)
            for sub_path, filepaths in sub_dirs.items():
                for filepath in filepaths:
                    files_per_path[filepath.name].append(data_path / sub_path)
            for filename, filepaths in files_per_path.items():
                if len(filepaths) > 1:
                    self.errors.append(_log(
                        f"{filename} has been found multiple times during this validation of path {data_path}. Duplicates: {filepaths}."))  # noqa: E501

    def _find_shared_prefixes(self, lock):
        # This pattern seeks out the string that includes the lane number (since
        # that is expected to be present to help anchor the prefix) that comes
        # before any of _I1, _I2, _R1, or _R2.
        fastq_file_prefix_regex = re.compile(r'(.+_L\d+.*)_[IR][12][._]')
        for fastq_file, records_read in self._file_record_counts.items():
            match = fastq_file_prefix_regex.match(Path(fastq_file).name)
            with lock:
                if match:
                    filename_prefix = match.group(1)
                    if filename_prefix in self._file_prefix_counts.keys():
                        extant_count = self._file_prefix_counts[filename_prefix]
                        if extant_count != records_read:
                            # Find a file we've validated already that matches this
                            # prefix.
                            extant_files = [
                                str(Path(filepath).name) for filepath, record_count
                                in self._file_record_counts.items()
                                if record_count == extant_count and Path(filepath).name.startswith(filename_prefix)
                            ]
                            # Based on how the dictionaries are created, there should
                            # always be at least one matching filename.
                            assert extant_files

                            self.errors.append(_log(
                                f"{Path(fastq_file).name} ({records_read} lines) "
                                f"does not match length of {extant_files[0]} "
                                f"({extant_count} lines)."))
                    else:
                        self._file_prefix_counts[filename_prefix] = records_read


def main():
    parser = argparse.ArgumentParser(description='Validate FASTQ files.')
    parser.add_argument('filepaths', type=Path, nargs='+',
                        help="Files to validate for FASTQ syntax")

    args = parser.parse_args()
    if isinstance(args.filepaths, List):
        filepaths = [Path(path) for path in args.filepaths]
    elif isinstance(args.filepaths, Path):
        filepaths = [args.filepaths]
    elif isinstance(args.filepaths, str):
        filepaths = [Path(args.filepaths)]
    else:
        raise Exception(
            f"Validator init received base_paths arg as type {type(args.filepaths)}"
        )

    validator = FASTQValidatorLogic(True)
    validator.validate_fastq_files_in_path(filepaths, Lock())


if __name__ == '__main__':
    main()
