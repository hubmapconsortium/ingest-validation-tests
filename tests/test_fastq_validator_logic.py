import gzip
from pathlib import Path
from typing import TextIO

import pytest

from src.ingest_validation_tests.fastq_validator_logic import FASTQValidatorLogic

_GOOD_RECORDS = """\
@A12345:123:A12BCDEFG:1:1234:1000:1234 1:N:0:NACTGACTGA+CTGACTGACT
NACTGACTGA
+
#FFFFFFFFF
"""

_GOOD_QUALITY_RECORD = (
    "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    r"[\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
)

_GOOD_SEQUENCE_FOR_QUALITY = (
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)


def _open_output_file(filename: Path, use_gzip: bool) -> TextIO:
    return gzip.open(filename, "wt") if use_gzip else open(filename, "wt")


class TestFASTQValidatorLogic:
    @pytest.fixture
    def fastq_validator(self) -> FASTQValidatorLogic:
        return FASTQValidatorLogic()

    def test_fastq_validator_no_files(self, fastq_validator, tmp_path):
        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        # This case should return no errors
        assert fastq_validator.errors == []

    def test_fastq_validator_bad_gzip_data(self, fastq_validator, tmp_path):
        # Note that the filename ends in .gz, although it will not contain
        # compressed data.
        test_file = tmp_path.joinpath("test.fastq.gz")
        with _open_output_file(test_file, False) as output:
            output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_file(test_file)
        assert "Bad gzip file" in fastq_validator.errors[0]

    def test_fastq_validator_unrecognized_file(self, fastq_validator, tmp_path):
        test_file = tmp_path.joinpath("test.txt")
        with _open_output_file(test_file, False) as output:
            output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_file(test_file)
        assert "Filename does not have proper format" in fastq_validator.errors[0]

    def test_fastq_validator_empty_directory(self, fastq_validator, tmp_path):
        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        # No files in path means no errors
        assert fastq_validator.errors == []

    @pytest.mark.parametrize("use_gzip", [False, True])
    def test_fastq_validator_basic(self, fastq_validator, tmp_path, use_gzip):
        test_file = tmp_path.joinpath("test.fastq.gz" if use_gzip else "test.fastq")
        with _open_output_file(test_file, use_gzip) as output:
            output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        assert not fastq_validator.errors

    def test_fastq_validator_bad_file(self, fastq_validator, tmp_path):
        test_file = tmp_path.joinpath("test.fastq")
        with _open_output_file(test_file, False) as output:
            output.write("ABCDEF")

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)

        # This test does not assert specific validations but instead that the
        # overall file failed and that error messages were returned.
        assert fastq_validator.errors

    def test_fastq_validator_duplicate_file(self, fastq_validator, tmp_path):
        for subdirectory in ["a", "b"]:
            subdirectory_path = tmp_path.joinpath(subdirectory)
            subdirectory_path.mkdir()
            with _open_output_file(subdirectory_path.joinpath("test.fastq"), False) as output:
                output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        assert "test.fastq has been found multiple times" in fastq_validator.errors[0]

    def test_fastq_validator_io_error(self, fastq_validator, tmp_path):
        fake_path = tmp_path.joinpath("does-not-exist.fastq")

        fastq_validator.validate_fastq_file(fake_path)

        assert "Unable to open" in fastq_validator.errors[0]

    def test_fastq_validator_line_1_good(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("@SEQ_ID", 0)

        assert not result

    def test_fastq_validator_line_1_bad(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("*SEQ_ID", 0)

        assert "does not begin with '@'" in result[0]

    def test_fastq_validator_line_1_empty(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("", 0)

        assert "does not begin with '@'" in result[0]

    def test_fastq_validator_line_2_good(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("ACTGACTGACTGNNNN", 1)

        assert not result

    def test_fastq_validator_line_2_bad(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("ACTGACT$ACTGNNNN", 1)

        assert "contains invalid character(s): $" in result[0]

    def test_fastq_validator_line_3_good(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("+SEQ_ID", 2)

        assert not result

    def test_fastq_validator_line_3_bad(self, fastq_validator):
        result = fastq_validator.validate_fastq_record("!SEQ_ID", 2)

        assert "does not begin with '+'" in result[0]

    def test_fastq_validator_line_4_good(self, fastq_validator):
        fastq_validator.validate_fastq_record(_GOOD_SEQUENCE_FOR_QUALITY, 1)
        result = fastq_validator.validate_fastq_record(_GOOD_QUALITY_RECORD, 3)

        assert not result

    def test_fastq_validator_line_4_bad(self, fastq_validator):
        fastq_validator.validate_fastq_record("1234567", 1)
        result = fastq_validator.validate_fastq_record("ABC !@#", 3)

        assert 'contains invalid quality character(s): " "' in result[0]

    def test_fastq_validator_line_4_matching_length(self, fastq_validator):
        fastq_validator.validate_fastq_record("1234567", 1)
        result = fastq_validator.validate_fastq_record("ABCDEFG", 3)

        assert not result

    def test_fastq_validator_line_4_mismatched_length(self, fastq_validator, tmp_path):
        fastq_validator.validate_fastq_record("123456789ABCDEF", 1)
        fastq_validator.validate_fastq_record("ABC", 3)

        test_data = """\
@A12345:123:A12BCDEFG:1:1234:1000:1234 1:N:0:NACTGACTGA+CTGACTGACT
NACTGACTGA
+
#FFFFFFFF
"""

        new_file = tmp_path.joinpath("test.fastq")
        with _open_output_file(new_file, False) as output:
            output.write(test_data)

        fastq_validator.validate_fastq_file(new_file)
        assert (
            "contains 9 characters which does not match line 2's 10" in fastq_validator.errors[0]
        )

    def test_fastq_validator_record_counts_good(self, fastq_validator, tmp_path):
        for filename in [
            "SREQ-1_1-ACTGACTGAC-TGACTGACTG_S1_L001_I1_001.fastq",
            "SREQ-1_1-ACTGACTGAC-TGACTGACTG_S1_L001_I2_001.fastq",
        ]:
            new_file = tmp_path.joinpath(filename)
            with _open_output_file(new_file, False) as output:
                output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)

        assert not fastq_validator.errors

    def test_fastq_validator_record_counts_bad(self, fastq_validator, tmp_path):
        with _open_output_file(
            tmp_path.joinpath("SREQ-1_1-ACTGACTGAC-TGACTGACTG_S1_L001_I1_001.fastq"), False
        ) as output:
            output.write(_GOOD_RECORDS)
        with _open_output_file(
            tmp_path.joinpath("SREQ-1_1-ACTGACTGAC-TGACTGACTG_S1_L001_I2_001.fastq"), False
        ) as output:
            output.write(_GOOD_RECORDS)
            output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)

        # Order of the files being processed is not guaranteed, however these
        # strings ensure that a mismatch was found.
        assert "(4 lines)" in fastq_validator.errors[0]
        assert "does not match" in fastq_validator.errors[0]
        assert "(8 lines)" in fastq_validator.errors[0]
