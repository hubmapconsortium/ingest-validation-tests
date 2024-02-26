import json
from pathlib import Path
from typing import List

from ingest_validation_tools.plugin_validator import Validator
from jsonschema import validate


class CodexJsonValidator(Validator):
    description = "Check CODEX JSON against schema"
    cost = 1.0
    required = "codex"

    def collect_errors(self, **kwargs) -> List[str]:
        del kwargs
        if self.required not in self.contains and self.assay_type.lower() != self.required:
            return []  # We only test CODEX data

        schema_path = Path(__file__).parent / "codex_schema.json"
        schema = json.loads(schema_path.read_text())

        rslt = []
        for glob_expr in ["**/dataset.json"]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    instance = json.loads(file.read_text())
                    try:
                        validate(instance=instance, schema=schema)
                    except Exception as e:
                        rslt.append(f"{file}: {e}")
        return rslt
