import argparse
import gzip
import logging
import re
from collections import defaultdict, namedtuple
from multiprocessing import Manager, Pool
from multiprocessing.managers import ListProxy
from os import cpu_count
from pathlib import Path
from typing import Callable, List, Optional, TextIO, Union

import fastq_utils
from typing_extensions import Self

filename_pattern = namedtuple("filename_pattern", ["prefix", "read_type", "set_num"])


def is_valid_filename(filename: str) -> bool:
    return bool(fastq_utils.FASTQ_PATTERN.fullmatch(filename))


def get_prefix_read_type_and_set(filename: str) -> Optional[filename_pattern]:
    """
    Looking for fastq filenames matching pattern:
    <prefix>_<lane:L#+>_<read_type:I,R,read>#_<set_num:#+>
    e.g. arbitrary_string_L001_R1_001.fastq

    Regex documentation:
        PREFIX
            - (?P<prefix> | named capture group "prefix"
            - .*(?:L\\d+) | match anything before and including pattern L# (where # represents 1 or more digits)
            - only capture prefix if read_type and set_num found
        READ_TYPE
            - (?=_ | assert that the character "_" is present at the beginning of this next group
            - (?:(?P<read_type>R|read(?=[123])|I(?=[12]))) | only capture above match if followed by the
               sequence _[R1,R2,R3,read1,read2,read3,I1,I2]; capture R/I/read as read_type, discarding digit
        SET_NUM
            - (?:\\d_ | ensure presence of "#_" before group, ignore characters
            - (?P<set_num>\\d+) | capture group set_num of 1 or more digits
               Note: if set_num needs to be optional, could change this portion to (?:\\d_?(?P<set_num>\\d+)|)
    """
    if not bool(fastq_utils.FASTQ_PATTERN.fullmatch(filename)):
        return
    pattern = re.compile(
        r"(?P<prefix>.*(?:L\d+)(?=_(?P<read_type>R|read(?=[123])|I(?=[12]))(?:\d_(?P<set_num>\d+))))"
    )
    groups = pattern.match(filename)
    if groups and all(x in groups.groupdict().keys() for x in ["prefix", "read_type", "set_num"]):
        return filename_pattern(
            groups.group("prefix"), groups.group("read_type"), groups.group("set_num")
        )


def _open_fastq_file(file: Path) -> TextIO:
    return gzip.open(file, "rt") if file.name.endswith(".gz") else file.open()


def _log(message: str, verbose: bool = True) -> Optional[str]:
    if verbose:
        print(message)
        return message


class Engine(object):
    def __init__(self, validate_object):
        self.validate_object = validate_object

    def __call__(self, fastq_file) -> list[Optional[str]]:
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

    _FASTQ_LINE_2_VALID_CHARS = "ACGNT"

    def __init__(self, verbose=False):
        self.errors: List[Optional[str]] = []
        self.files_were_found = False
        self.file_list = Manager().list()
        self._file_record_counts = Manager().dict()
        self._ungrouped_files = Manager().list()
        self._filename = ""
        self._line_number = 0

        self._verbose = verbose

        self._line_2_length = 0
        self._last_line_2_number = 0

    def _format_error(self, error: str) -> str:
        location = self._filename
        if self._line_number:
            location += f":{self._line_number}"

        message = f"{location}: {error}"

        print(message)
        return message

    def _validate_fastq_line_1(self, line: str) -> List[str]:
        if not line or line[0] != "@":
            return ["Line does not begin with '@'."]

        return []

    def _validate_fastq_line_2(self, line: str) -> List[str]:
        self._line_2_length = len(line)
        self._last_line_2_number = self._line_number

        invalid_chars = "".join(c for c in line if c not in self._FASTQ_LINE_2_VALID_CHARS)
        if invalid_chars:
            return [f"Line contains invalid character(s): {invalid_chars}"]

        return []

    def _validate_fastq_line_3(self, line: str) -> List[str]:
        if not line or line[0] != "+":
            return ["Line does not begin with '+'."]

        return []

    def _validate_fastq_line_4(self, line: str) -> List[str]:
        errors: List[str] = []
        invalid_chars = "".join(c for c in line if not 33 <= ord(c) <= 126)
        if invalid_chars:
            errors.append("Line contains invalid quality character(s): " f'"{invalid_chars}"')

        if len(line) != self._line_2_length:
            errors.append(
                f"Line contains {len(line)} characters which "
                f"does not match line {self._last_line_2_number}'s "
                f"{self._line_2_length} characters."
            )
        return errors

    _VALIDATE_FASTQ_LINE_METHODS = {
        1: _validate_fastq_line_1,
        2: _validate_fastq_line_2,
        3: _validate_fastq_line_3,
        4: _validate_fastq_line_4,
    }

    def validate_fastq_record(self, line: str, line_number: int) -> List[str]:
        line_index = line_number % 4 + 1

        validator_method: Callable[[Self, str], List[str]] = self._VALIDATE_FASTQ_LINE_METHODS[
            line_index
        ]

        assert validator_method, f"No validator method defined for record index {line_index}"

        return validator_method(self, line)

    def validate_fastq_stream(self, fastq_data: TextIO) -> int:
        # Returns the number of records read from fastq_data.
        line_count = 0

        line: str
        for line_count, line in enumerate(fastq_data):
            self._line_number = line_count + 1
            self.errors.extend(
                self._format_error(error)
                for error in self.validate_fastq_record(line.rstrip(), line_count)
            )

        return line_count + 1

    def validate_fastq_file(self, fastq_file: Path) -> None:
        _log(f"Validating {fastq_file.name}...")
        _log(f"    → {fastq_file.absolute().as_posix()}")

        if not is_valid_filename(fastq_file.name):
            # If we don't like the filename, don't bother reading the contents.
            self.errors.append(
                _log("Filename does not have proper format " "and will not be processed")
            )
            return

        self._line_number = 0
        self._filename = fastq_file.name

        try:
            with _open_fastq_file(fastq_file) as fastq_data:
                records_read = self.validate_fastq_stream(fastq_data)
        except gzip.BadGzipFile:
            self.errors.append(self._format_error(f"Bad gzip file: {fastq_file}."))
            return
        except IOError:
            self.errors.append(self._format_error(f"Unable to open FASTQ data file {fastq_file}."))
            return
        except EOFError:
            self.errors.append(self._format_error(f"EOF in FASTQ data file {fastq_file}."))
            return
        except Exception as e:
            self.errors.append(
                self._format_error(f"Unexpected error: {e} on data file {fastq_file}.")
            )
            return
        self._file_record_counts[str(fastq_file)] = records_read

    def validate_fastq_files_in_path(self, paths: List[Path], threads: int) -> None:
        """
        - Builds a list of filepaths; checks for duplicate filenames in upload.
        - [parallel] Opens, validates, and gets line count of each file in list, and then
        populates self._file_record_counts as {filepath: record_count}.
        - If successful, groups files with matching prefix/read_type/set_num values.
        - Compares record_counts across grouped files, logs any that don't match or are ungrouped.
        """
        for path in paths:
            fastq_utils_output = fastq_utils.collect_fastq_files_by_directory(path)
            for files in fastq_utils_output.values():
                self.file_list.extend(files)
                _log(f"FASTQValidatorLogic: Added files from {path} to file_list: {files}")
        self.files_were_found = bool(self.file_list)
        data_found_one = []
        with Manager() as manager:
            lock = manager.Lock()
            pool = Pool(threads)
            try:
                logging.info(
                    f"Passing file list for paths {self._printable_filenames(paths, newlines=False)} to engine. File list:"
                )
                logging.info(self._printable_filenames(self.file_list, newlines=True))
                engine = Engine(self)
                data_output = pool.imap_unordered(engine, self.file_list)
                [data_found_one.extend(output) for output in data_output if output]
            except Exception as e:
                pool.close()
                pool.join()
                _log(f"Error {e}")
                self.errors.append(f"Error {e}")
            else:
                pool.close()
                pool.join()
                self._find_duplicates()
                groups = self._make_groups()
                self._find_counts(groups, lock)
                if self._ungrouped_files:
                    _log(f"Ungrouped files, counts not checked: {self._ungrouped_files}")
        if len(data_found_one) > 0:
            self.errors.extend(data_found_one)

    def _make_groups(self) -> dict[filename_pattern, list[Path]]:
        groups = defaultdict(list)
        for file in self.file_list:
            potential_match = get_prefix_read_type_and_set(file.name)
            if potential_match:
                groups[potential_match].append(file)
            else:
                self._ungrouped_files.append(file)
        for group in groups.values():
            group.sort()
        return groups

    def _find_duplicates(self):
        """
        Transforms data from {path: [filepaths]} to {filepath.name: [paths]}
        to ensure that each filename only appears once in an upload
        """
        files_per_path = defaultdict(list)
        for filepath in self.file_list:
            files_per_path[filepath.name].append(filepath.parents[0])
        for filename, filepaths in files_per_path.items():
            if len(filepaths) > 1:
                self.errors.append(
                    _log(
                        f"{filename} has been found multiple times during this validation. "
                        f"Locations of duplicates: {str(filepaths)}."
                    )
                )

    def _find_counts(self, groups: dict[filename_pattern, list[Path]], lock):
        with lock:
            for pattern, paths in groups.items():
                if len(paths) == 1:
                    # This would happen if there was a file that matched the prefix_read_set pattern
                    # but did not have a counterpart for comparison; this probably should not happen but
                    # is currently only logged and does not throw an exception
                    self._ungrouped_files.append(paths[0])
                    continue
                comparison = {}
                for path in paths:
                    comparison[str(path)] = self._file_record_counts.get(str(path))
                if not (len(set(comparison.values())) == 1):
                    self.errors.append(
                        f"Counts do not match among files matching pattern {pattern.prefix}_{pattern.read_type}#_{pattern.set_num}: {comparison}"
                    )
                else:
                    _log(
                        f"PASSED: Record count comparison for files matching pattern {pattern.prefix}_{pattern.read_type}#_{pattern.set_num}: {comparison}"
                    )

    def _printable_filenames(
        self, files: Union[list, ListProxy, Path, str], newlines: bool = True
    ):
        if type(files) is list or type(files) is ListProxy:
            file_list = [str(file) for file in files]
            if newlines:
                return "\n".join(file_list)
            return file_list
        elif type(files) is Path:
            return str(files)
        elif type(files) is str:
            return files


def main():
    parser = argparse.ArgumentParser(description="Validate FASTQ files.")
    parser.add_argument(
        "filepaths", type=Path, nargs="+", help="Files to validate for FASTQ syntax"
    )
    parser.add_argument("coreuse", type=int, help="Number of cores to use")

    args = parser.parse_args()
    if isinstance(args.filepaths, List):
        filepaths = [Path(path) for path in args.filepaths]
    elif isinstance(args.filepaths, Path):
        filepaths = [args.filepaths]
    elif isinstance(args.filepaths, str):
        filepaths = [Path(args.filepaths)]
    else:
        raise Exception(f"Validator init received base_paths arg as type {type(args.filepaths)}")

    validator = FASTQValidatorLogic(True)
    if not (threads := args.coreuse):
        cpus = cpu_count()
        threads = cpus // 4 if cpus else 1
    validator.validate_fastq_files_in_path(filepaths, threads)


if __name__ == "__main__":
    main()
