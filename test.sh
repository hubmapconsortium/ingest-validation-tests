#!/usr/bin/env bash

# Accepts additional args to pass to pytest
# Accepts flags as individual args as well as
# custom "--test=" arg which can be used to run a specific
# test file/class/test just as you would pass it to pytest directly.
# Example:
# ./test.sh "--test=tests/test_fastq_validator_logic.py::TestFASTQValidatorLogic::test_fastq_groups_good" --pdb

set -o errexit

red="$(tput setaf 1)"
green="$(tput setaf 2)"
reset="$(tput sgr0)"

start() { [[ -z "$CI" ]] || echo "travis_fold:start:$1"; echo "$green$1$reset"; }
end() { [[ -z "$CI" ]] || echo "travis_fold:end:$1"; }
die() { set +v; echo "$red$*$reset" 1>&2 ; exit 1; }


path_to_tools='../ingest-validation-tools'
start placeholder
${path_to_tools}/src/validate_upload.py --help > /dev/null \
    || die 'validate_upload.py failed'
python tests/pytest_runner.py ${path_to_tools} "$@"
end placeholder
