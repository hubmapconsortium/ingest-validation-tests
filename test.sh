#!/usr/bin/env bash
# Accepts flags as individual args as well as custom "--test=" arg
# which can be used to run a specific test file/class/test just as you
# would pass it to pytest directly.

# Example:
# ./test.sh "--test=tests/test_fastq_validator_logic.py::TestFASTQValidatorLogic::test_fastq_groups_good" --pdb
#
set -o errexit

python tests/pytest_runner.py ${path_to_tools} "$@"
