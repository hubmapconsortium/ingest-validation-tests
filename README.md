# ingest-validation-tests

This repository contains plug-in tests for use during validation of submissions. It is referenced by ingest-validation-tools.

## Development process

### Branches

- Make new feature branches from `devel`.
- Before submitting a PR, make sure your code is black, isort, and flake8 compliant. Run the following from the base `ingest-validation-tests` directory:

  ```
  black --line-length 99 .
  isort --profile black --multi-line 3 .
  flake8
  ```

  (Integrating black and potentially isort/flake8 with your editor may allow you to skip this step, see Setup section below.)

- Make PRs to `devel`. (This is the default branch.)
- The last reviewer to approve a PR should merge it.

### Setup

- Creating and activating a virtual environment is recommended. These instructions assume you are using a virtual environment. Example using venv:

  ```
  python3.9 -m venv hm-ingest-validation-tests
  source hm-ingest-validation-tests/bin/activate
  ```

- Run `pip install -r requirements-dev.txt`
- (optional) Integrate black with your editor.
  - [Instructions for black.](https://black.readthedocs.io/en/stable/integrations/editors.html)
- (optional) Integrate [isort](https://pycqa.github.io/isort/) with your editor.
- (optional) Integrate [flake8](https://flake8.pycqa.org/en/latest/index.html) with your editor.

### Testing

- If ingest-validation-tools is not already set up:

  ```
  # Starting from ingest-validation-tests...
  cd ..
  git clone https://github.com/hubmapconsortium/ingest-validation-tools.git
  cd ingest-validation-tests
  pip install -r ../ingest-validation-tools/requirements.txt
  pip install -r ../ingest-validation-tools/requirements-dev.txt
  ```

- If ingest-validation-tools is already set up, add the appropriate ingest-validation-tools path and run:

  ```
  pip install -r <path-to-ingest-validation-tools>/requirements.txt
  pip install -r <path-to-ingest-validation-tools>/requirements-dev.txt
  ```

- Run `test.sh`
