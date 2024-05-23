"""
Test for some common errors in the directory and file structure of publications.
"""

import json
import re
from pathlib import Path
from typing import List, Optional

import frontmatter
from ingest_validation_tools.plugin_validator import Validator


class PublicationValidator(Validator):
    """
    Test for some common errors in the directory and file structure of
    publications.
    """

    description = "Test for common problems found in publications"
    cost = 1.0
    version = "1.0"
    base_url_re = r"(\s*\{\{\s*base_url\s*\}\})/(.*)"
    url_re = r"[Uu][Rr][Ll]"
    required = "publication"

    def collect_errors(self, **kwargs) -> List[Optional[str]]:
        """
        Return the errors found by this validator
        """
        del kwargs
        if self.required not in self.contains and self.assay_type.lower() != self.required:
            return []  # We only test Publication data
        rslt = []
        for path in self.paths:
            try:
                vignette_path = path / "vignettes"
                assert vignette_path.is_dir(), "vignettes not found or not a directory"
                for this_vignette_path in vignette_path.glob("*"):
                    assert this_vignette_path.is_dir(), (
                        f"Found the non-dir {this_vignette_path}" " in vignettes"
                    )
                    this_vignette_all_paths = set(this_vignette_path.glob("*"))
                    if not all(pth.is_file() for pth in this_vignette_all_paths):
                        raise AssertionError("Found a subdirectory in a vignette")
                    md_found = False
                    vig_figures = []
                    for md_path in this_vignette_path.glob("*.md"):
                        if md_found:
                            raise AssertionError("A vignette has more than one markdown file")
                        else:
                            md_found = True
                        vig_fm = frontmatter.loads(md_path.read_text())
                        for key in ["name", "figures"]:
                            assert key in vig_fm.metadata, (
                                "vignette markdown is incorrectly" f" formatted or has no {key}"
                            )
                        for fig_dict in vig_fm.metadata["figures"]:
                            assert "file" in fig_dict, "figure dict does not reference a file"
                            assert "name" in fig_dict, "figure dict does not provide a name"
                            vig_figures.append(fig_dict["file"])
                        this_vignette_all_paths.remove(md_path)
                        for fname in vig_figures:
                            rslt.extend(
                                self.validate_vitessce_config(this_vignette_path / fname, path)
                            )
                            this_vignette_all_paths.remove(this_vignette_path / fname)
                    assert not this_vignette_all_paths, (
                        "unexpected files in vignette:"
                        f" {list(str(elt) for elt in this_vignette_all_paths)}"
                    )

            except AssertionError as excp:
                rslt.append(str(excp))

        if rslt:
            return rslt
        elif self.paths:
            return [None]
        else:
            return []

    def url_search_iter(self, root):
        if isinstance(root, list):
            for elt in root:
                for key, val in self.url_search_iter(elt):
                    yield key, val
        elif isinstance(root, dict):
            for elt in root:
                if re.search(self.url_re, elt):
                    val = root[elt]
                    yield elt, val
                else:
                    for key, val in self.url_search_iter(root[elt]):
                        yield key, val
        elif isinstance(root, (str, int, float)):
            pass
        elif root is None:
            pass
        else:
            raise AssertionError(f"what is {root} of type {type(root)} ?")

    def validate_vitessce_config(self, json_path, path):
        rslt = []
        with open(json_path) as f:
            dct = json.load(f)
            for _, val in self.url_search_iter(dct):
                try:
                    match = re.match(self.base_url_re, val)
                    if match:  # it starts with {{ base_url }}
                        extra_url = match.group(2)
                        data_path = path / "data" / extra_url
                        assert data_path.exists(), (
                            "expected data file" f" {Path('data') / extra_url} is absent"
                        )

                except AssertionError as excp:
                    rslt.append(str(excp))
        return rslt
