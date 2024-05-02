import re
import zipfile
from pathlib import Path

import pytest
import semantic_version

from ingest_validation_tools.plugin_validator import validation_class_iter

FALLBACK_REGEX = r"^\d+\.\d+$"

def test_plugin_semantic_versions():
    plugin_dir = (Path(__file__).parent.parent
                  / "src" / "ingest_validation_tests")
    classnames = []
    fb_regex = re.compile(FALLBACK_REGEX)
    for cls in validation_class_iter(plugin_dir):
        validator = cls(['.'], 'someassay', plugin_dir, [])
        try:
            ver = semantic_version.Version(cls.version)
        except ValueError:
            if not fb_regex.match(cls.version):
                assert False, (f"The plugin {cls.__name__} has version string"
                               f" {cls.version}, which is not a valid semantic"
                               " version")
