import re

import semantic_version

FALLBACK_REGEX = r"^\d+\.\d+$"


def test_plugin_semantic_versions():
    from validator import validation_class_iter

    fb_regex = re.compile(FALLBACK_REGEX)
    for cls in validation_class_iter():
        try:
            _ = semantic_version.Version(cls.version)
        except ValueError:
            if not fb_regex.match(cls.version):
                assert False, (
                    f"The plugin {cls.__name__} has version string"
                    f" {cls.version}, which is not a valid semantic"
                    " version"
                )
