from typing import List
from pathlib import Path
import json

from jsonschema import validate

from ingest_validation_tools.plugin_validator import Validator


class CodexJsonValidator(Validator):
    description = "Check CODEX JSON against schema"
    cost = 1.0

    def collect_errors(self, **kwargs) -> List[str]:
        schema_path = Path(__file__).parent / 'codex_schema.json'
        schema = json.loads(schema_path.read_text())

        rslt = []
        for glob_expr in ['**/dataset.json']:
            for path in self.path.glob(glob_expr):
                instance = json.loads(path.read_text())
                try:
                    validate(instance=instance, schema=schema)
                except Exception as e:
                    rslt.append(f'{path}: {e}')
        return rslt
