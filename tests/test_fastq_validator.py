import os
import sys

# Ensure access to submodules' Python module to gain full access to imports'
# dependencies.

sys.path.insert(0, os.path.abspath('../../..'))
from submodules import ingest_validation_tests
from ingest_validation_tests.fastq_validator import has_valid_name


class TestHasValidName:
    def test_has_valid_name_fastq_gz(self):
        assert has_valid_name('test.fastq.gz')

    def test_has_valid_name_fastq(self):
        assert has_valid_name('test.fastq')

    def test_has_valid_name_invalid_gz(self):
        assert not has_valid_name('test.gz')
