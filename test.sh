#!/usr/bin/env bash
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
python tests/pytest_runner.py ${path_to_tools}
end placeholder
