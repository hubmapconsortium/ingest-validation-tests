import gzip
import os
from pathlib import Path
import pytest
import sys
from typing import TextIO

# Ensure access to submodules' Python module to gain full access to imports'
# dependencies.

sys.path.insert(0, os.path.abspath('../../..'))
from submodules import ingest_validation_tests
from ingest_validation_tests.fastq_validator import has_valid_name, \
    FASTQValidator


class TestHasValidName:
    def test_has_valid_name_fastq_gz(self):
        assert has_valid_name('test.fastq.gz')

    def test_has_valid_name_fastq(self):
        assert has_valid_name('test.fastq')

    def test_has_valid_name_invalid_gz_filename(self):
        assert not has_valid_name('test.gz')


_ASSAY_TYPE = 'snRNAseq'

# 1000 below increments for subsequent records
_GOOD_RECORDS = '''\
@A12345:123:A12BCDEFG:1:1234:1000:1234 1:N:0:NACTGACTGA+CTGACTGACT
NACTGACTGA
+
#FFFFFFFFF
'''


class TestFASTQValidator:
    @pytest.fixture
    def fastq_validator(self, tmp_path) -> FASTQValidator:
        return FASTQValidator(tmp_path, _ASSAY_TYPE)

    def test_fastq_validator_no_files(self, fastq_validator):
        result = fastq_validator.collect_errors()
        assert "No files matching" in result[0]

    def _open_output_file(self, filename: Path, use_gzip: bool) -> TextIO:
        return (
            gzip.open(filename, 'wt') if use_gzip
            else open(filename, 'wt')
        )

    @pytest.mark.parametrize("use_gzip", [False, True])
    def test_fastq_validator_good_file(self, fastq_validator, tmp_path,
                                       use_gzip):
        test_file = tmp_path.joinpath('test.fastq')
        with self._open_output_file(test_file, False) as output:
            output.write(_GOOD_RECORDS)

        result = fastq_validator.collect_errors()
        assert not result
