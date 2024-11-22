import gzip
from operator import attrgetter
from pathlib import Path, PosixPath
from typing import TextIO

import pytest

from src.ingest_validation_tests.fastq_validator_logic import (
    FASTQValidatorLogic,
    filename_pattern,
    get_prefix_read_type_and_set,
)

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

        # Non-matching records only stored in errors, need to do ugly string match
        assert "Counts do not match" in fastq_validator.errors[0]
        assert (
            "SREQ-1_1-ACTGACTGAC-TGACTGACTG_S1_L001_I1_001.fastq': 4" in fastq_validator.errors[0]
        )
        assert (
            "SREQ-1_1-ACTGACTGAC-TGACTGACTG_S1_L001_I2_001.fastq': 8" in fastq_validator.errors[0]
        )

    def test_fastq_comparison_good(self, fastq_validator, tmp_path):
        filenames = [
            "3252_ftL_RNA_T1_S31_L003_R1_001.fastq",
            "3252_ftL_RNA_T1_S31_L003_R2_001.fastq",
            "3252_ftL_RNA_T1_S31_L003_R1_002.fastq",
            "3252_ftL_RNA_T1_S31_L003_R2_002.fastq",
        ]
        for filename in filenames:
            new_file = tmp_path.joinpath(filename)
            with _open_output_file(new_file, False) as output:
                output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)

        assert not fastq_validator.errors

    def test_fastq_comparison_bad_unequal_line_counts(self, fastq_validator, tmp_path):
        good_file = "3252_ftL_RNA_T1_S31_L003_R1_001.fastq"
        bad_file = "3252_ftL_RNA_T1_S31_L003_R2_001.fastq"
        new_good_file = tmp_path.joinpath(good_file)
        with _open_output_file(new_good_file, False) as output:
            output.write(_GOOD_RECORDS)
        new_bad_file = tmp_path.joinpath(bad_file)
        with _open_output_file(new_bad_file, False) as output:
            output.write("bad")

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)

        assert fastq_validator.errors

    def test_fastq_groups_good(self, fastq_validator, tmp_path):
        files = [
            PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R1_001.fastq")),
            PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R2_001.fastq")),
            PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R3_001.fastq")),
            PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R1_002.fastq")),
            PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R2_002.fastq")),
        ]
        for file in files:
            with _open_output_file(tmp_path.joinpath(file), False) as output:
                output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        assert fastq_validator._make_groups(files) == {
            filename_pattern(
                before_read="20147_Healthy_PA_S1_L001_", read="R", after_read="_001.fastq"
            ): [
                PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R1_001.fastq")),
                PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R2_001.fastq")),
                PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R3_001.fastq")),
            ],
            filename_pattern(
                before_read="20147_Healthy_PA_S1_L001_", read="R", after_read="_002.fastq"
            ): [
                PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R1_002.fastq")),
                PosixPath(tmp_path.joinpath("20147_Healthy_PA_S1_L001_R2_002.fastq")),
            ],
        }
        assert not fastq_validator.errors

    def test_fastq_groups_bad(self, fastq_validator, tmp_path):
        good_files = [
            "20147_Healthy_PA_S1_L001_R1_001.fastq",
            "20147_Healthy_PA_S1_L001_R2_001.fastq",
            "20147_Healthy_PA_S1_L001_R1_002.fastq",
            "test_not_grouped_but_valid",
        ]
        bad_files = [
            "20147_Healthy_PA_S1_L001_R3_001.fastq",
            "20147_Healthy_PA_S1_L001_R2_002.fastq",
        ]
        for file in good_files:
            with _open_output_file(tmp_path.joinpath(file), False) as output:
                output.write(_GOOD_RECORDS)
        for file in bad_files:
            with _open_output_file(tmp_path.joinpath(file), False) as output:
                output.write("@bad")

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        sorted_errors = sorted(fastq_validator.errors)
        assert "Counts do not match" in sorted_errors[0]
        assert "20147_Healthy_PA_S1_L001_R3_001.fastq" in sorted_errors[0]
        assert "Counts do not match" in sorted_errors[1]
        assert "20147_Healthy_PA_S1_L001_R2_002.fastq" in sorted_errors[1]

    def test_filename_valid_and_fastq_valid_but_not_grouped(self, fastq_validator, tmp_path):
        # good_filenames[0:5] are valid in pipeline processing but would not be grouped for comparison
        good_filenames = [
            "B001A001_1.fastq",  # no lane, read_type, or set
            "B001A001_R1.fq",  # no lane or set
            "B001A001_I1.fastq.gz",  # no lane or set
            "H4L1-4_S64_R1_001.fastq.gz",  # no lane
            "H4L1-4_S64_L001_001.fastq.gz",  # no read_type
            "H4L1-4_S64_L001_R1.fastq.gz",  # will try to group; no set
            "L001_H4L1-4_S64_R1_001.fastq.gz",  # will try to group; out of order but has required elements
            "H4L1-4_S64_L001_R1_001.fastq.gz",  # will try to group; will be compared
            "H4L1-4_S64_L001_R2_001.fastq.gz",  # will try to group; will be compared
            "H4L1-4_S64_L001_I1_001.fastq.gz",  # will try to group; read type `I`
            "Undetermined_S0_L001_R1_001.W105_Small_bowel_ileum.trimmed.fastq.gz",  # will try to group; annotated but otherwise fits pattern
        ]
        for file in good_filenames:
            use_gzip = bool("gz" in file)
            with _open_output_file(tmp_path.joinpath(file), use_gzip) as output:
                output.write(_GOOD_RECORDS)

        fastq_validator.validate_fastq_files_in_path([tmp_path], 2)
        # All files in good_filenames should be in files_by_path
        assert {
            PosixPath(tmp_path.joinpath(file)) in fastq_validator.files_by_path[tmp_path]
            for file in good_filenames
        } == {True}
        # No errors should be found in any of those files
        assert not fastq_validator.errors
        # Only some files from good_filenames will match criteria for grouping
        valid_filename_patterns = [
            get_prefix_read_type_and_set(str(file))
            for file in fastq_validator.files_by_path[tmp_path]
            if get_prefix_read_type_and_set(str(file)) is not None
        ]
        assert sorted(
            valid_filename_patterns, key=attrgetter("before_read", "read", "after_read")
        ) == sorted(
            [
                filename_pattern(
                    before_read=f"{tmp_path}/H4L1-4_S64_L001_", read="R", after_read=".fastq.gz"
                ),
                filename_pattern(
                    before_read=f"{tmp_path}/L001_H4L1-4_S64_",
                    read="R",
                    after_read="_001.fastq.gz",
                ),
                filename_pattern(
                    before_read=f"{tmp_path}/H4L1-4_S64_L001_",
                    read="R",
                    after_read="_001.fastq.gz",
                ),
                filename_pattern(
                    before_read=f"{tmp_path}/H4L1-4_S64_L001_",
                    read="R",
                    after_read="_001.fastq.gz",
                ),
                filename_pattern(
                    before_read=f"{tmp_path}/H4L1-4_S64_L001_",
                    read="I",
                    after_read="_001.fastq.gz",
                ),
                filename_pattern(
                    before_read=f"{tmp_path}/Undetermined_S0_L001_",
                    read="R",
                    after_read="_001.W105_Small_bowel_ileum.trimmed.fastq.gz",
                ),
            ],
            key=attrgetter("before_read", "read", "after_read"),
        )
        assert (
            fastq_validator._ungrouped_files.sort()
            == [
                "B001A001_1.fastq",  # no lane, read_type, or set
                "B001A001_R1.fq",  # no lane or set
                "B001A001_I1.fastq.gz",  # no lane or set
                "H4L1-4_S64_R1_001.fastq.gz",  # no lane
                "H4L1-4_S64_L001_001.fastq.gz",  # no read_type
                "H4L1-4_S64_L001_R1.fastq.gz",  # no set
                "L001_H4L1-4_S64_R1_001.fastq.gz",  # out of order
            ].sort()
        )

    def test_duplicate_filenames_in_different_top_level_paths_pass(
        self, fastq_validator, tmp_path
    ):
        good_files = [
            "data_dir_1/20147_Healthy_PA_S1_L001_R1_001.fastq",
            "data_dir_1/20147_Healthy_PA_S1_L001_R2_001.fastq",
            "data_dir_2/20147_Healthy_PA_S1_L001_R1_001.fastq",
            "data_dir_2/20147_Healthy_PA_S1_L001_R2_001.fastq",
        ]
        tmp_path.joinpath("data_dir_1").mkdir()
        tmp_path.joinpath("data_dir_2").mkdir()
        for file in good_files:
            with _open_output_file(tmp_path.joinpath(file), False) as output:
                output.write(_GOOD_RECORDS)
        fastq_validator.validate_fastq_files_in_path(
            [tmp_path.joinpath("data_dir_1"), tmp_path.joinpath("data_dir_2")], 2
        )
        assert not fastq_validator.errors
