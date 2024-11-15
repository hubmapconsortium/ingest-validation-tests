import sys
from pathlib import Path

import pytest


class add_path:
    """
    Add an element to sys.path using a context.
    Thanks to Eugene Yarmash https://stackoverflow.com/a/39855753
    """

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        sys.path.insert(0, self.path)

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            sys.path.remove(self.path)
        except ValueError:
            pass


def main():
    """
    Receives args from test.sh but can also accept pytest flags or
    custom --test= arg directly, e.g.
    python tests/pytest_runner.py '../ingest-validation-tools/' --test="tests/test_fastq_validator_logic.py::TestFASTQValidatorLogic::test_fastq_groups_good" --pdb
    """
    if len(sys.argv) < 2:
        sys.exit(f"usage: {sys.argv[0]} path-to-ingest-validation-tools")
    tools_path = Path(sys.argv[1]).resolve() / "src"
    plugins_path = Path(__file__).resolve().parent.parent / "src" / "ingest_validation_tests"
    with add_path(str(tools_path)):
        with add_path(str(plugins_path)):
            args = ["-vv"]
            for arg in sys.argv[2:]:
                if arg.startswith("--test="):
                    args.append(arg.replace("--test=", ""))
                else:
                    args.append(arg)
            sys.exit(pytest.main(args))


if __name__ == "__main__":
    main()
