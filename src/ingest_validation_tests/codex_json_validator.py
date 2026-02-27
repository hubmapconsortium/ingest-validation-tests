import json
from pathlib import Path

from jsonschema import validate
from jsonschema.exceptions import SchemaError, ValidationError
from validator import Validator


class CodexJsonValidator(Validator):
    description = "Check CODEX JSON against schema"
    cost = 1.0
    version = "1.0"
    required = ["codex"]

    def _collect_errors(self) -> list[str | None]:
        schema_path = Path(__file__).parent / "codex_schema.json"
        schema = json.loads(schema_path.read_text())

        rslt = []
        files_tested = 0
        for glob_expr in ["**/dataset.json"]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    files_tested += 1
                    instance = json.loads(file.read_text())
                    try:
                        validate(instance=instance, schema=schema)
                    except (ValidationError, SchemaError) as e:
                        self._log(e)
                        rslt.append(f"{file}: {e.__class__.__name__}: {e.message}")
        print(f"DONE <{rslt}> {files_tested}")
        return self._return_result(rslt, files_tested)
