import re
from pathlib import Path

from validator import Validator


class BarcodeValidator(Validator):
    description = "Validate characters in barcodes.txt"
    cost = 1.0
    version = "1.0"
    required = ["paired-tag"]
    only_allowed_regex = r"[^ACTG]"

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.errors: list = []

    def _collect_errors(self) -> list[str | None]:
        found = []
        for path in self.paths:
            if self.find_and_check_barcode_file(path):
                found.append(path)
        return self._return_result(self.errors, found)

    def find_and_check_barcode_file(self, path: Path) -> bool:
        barcode_file = path.joinpath("raw/barcodes.txt")
        if barcode_file.exists():
            with open(barcode_file, "r") as f:
                lines = f.read().splitlines()
                errors = [
                    str(i + 1)
                    for i, line in enumerate(lines)
                    if re.search(self.only_allowed_regex, line)
                ]
                if errors:
                    msg = f"Only characters 'A', 'C', 'T', 'G' allowed in {self.rel_filename_str(barcode_file)}."
                    if len(errors) <= 20:
                        self.errors.append(msg + f" Errors on lines {', '.join(errors)}.")
                    else:
                        self.errors.append(msg + f" {len(errors)} lines have errors.")
            return True
        return False
