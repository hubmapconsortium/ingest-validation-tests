import json
import re
from collections import defaultdict
from pathlib import Path

import frontmatter
from validator import Validator


class PublicationVignettesValidator(Validator):
    """
    Test for some common errors in the directory and file structure of
    publications.
    """

    description = (
        "Test for common problems found in the directory and file structure of publications"
    )
    cost = 1.0
    version = "1.0"
    base_url_re = r"(\s*\{\{\s*base_url\s*\}\})/(.*)"
    url_re = r"[Uu][Rr][Ll]"
    required = ["publication"]
    hint = "Please check the guidance document for formatting requirements: https://docs.google.com/document/d/1JosCXhMMFNdU0GEBk6cUCeT6VewaruKKB0Etr-wWSMo/edit?usp=sharing"

    def _collect_errors(self) -> list[str | None]:
        rslt = []
        for path in self.paths:
            vignettes_path = path / "vignettes"
            if not vignettes_path.exists():
                rslt.append({self.rel_filename_str(path): "Directory not found."})
                continue
            if not vignettes_path.is_dir():
                rslt.append({self.rel_filename_str(path): "Path is not a directory."})
                continue
            errors = []
            for vignette_dir in vignettes_path.glob("*"):
                if not vignette_dir.is_dir():
                    errors.append({self.rel_filename_str(vignette_dir): "Not a directory."})
                    continue
                if dir_errors := self._check_vignette_dir(vignette_dir, path):
                    errors.append({self.rel_filename_str(vignette_dir): dir_errors})
            rslt.extend(errors)
        print(rslt)
        return self._return_result(rslt, self.paths)

    def _check_vignette_dir(self, vignette_dir: Path, path: Path) -> list:
        """
        Vignettes directory should include one subdirectory for each vignette.
        Subdirectory should include description.md and any paths referenced by it.
        """
        errors = []
        all_paths_in_vignette = set(vignette_dir.glob("*"))
        if len(md_paths := list(vignette_dir.glob("*.md"))) > 1:
            errors.append(
                f"Vignette has more than one markdown file: {', '.join([str(path.relative_to(vignette_dir)) for path in md_paths])}"
            )
            return errors
        md_errors, modified_all_paths_in_vignette = self._check_vignette_md(
            md_paths[0], all_paths_in_vignette, vignette_dir, path
        )
        if md_errors:
            errors.append(md_errors)
        if modified_all_paths_in_vignette:
            if files := [
                str(self.rel_filename_str(elt))
                for elt in all_paths_in_vignette
                if not elt.is_dir()
            ]:
                errors.append(f"Unexpected files in vignette: {', '.join(files)}")
            if files := [
                str(self.rel_filename_str(elt)) for elt in all_paths_in_vignette if elt.is_dir()
            ]:
                errors.append(f"Found subdirectories in vignette: {', '.join(files)}")
        return errors

    def _check_vignette_md(
        self, md_path: Path, all_paths_in_vignette: set[Path], vignette_dir: Path, path: Path
    ) -> tuple[dict, set[Path]]:
        errors = defaultdict(list)
        vig_fm = frontmatter.loads(md_path.read_text())
        if "name" not in vig_fm.metadata and "figures" not in vig_fm.metadata:
            errors[self.rel_filename_str(md_path)].append(
                f"Vignette markdown is incorrectly formatted or missing required elements. 'name': {vig_fm.get('name')}, 'figures': {vig_fm.get('figures')}"
            )
        vig_figures = []
        rel_md_path = str(md_path.relative_to(vignette_dir))
        for fig_dict in vig_fm.get("figures", []):
            for key in ["file", "name"]:
                if key not in fig_dict:
                    errors[rel_md_path].append(f"'figure' dict missing required element '{key}'.")
            if fig_dict.get("file"):
                vig_figures.append(fig_dict["file"])
        all_paths_in_vignette.remove(md_path)
        for fname in vig_figures:
            if file_errors := self.validate_vitessce_config(vignette_dir / fname, path):
                errors[rel_md_path].extend(file_errors)
            all_paths_in_vignette.remove(vignette_dir / fname)
        if errors:
            errors["hint"] = [self.hint]
        return dict(errors) if errors else {}, all_paths_in_vignette

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

    def validate_vitessce_config(self, json_path, path) -> list:
        rslt = []
        with open(json_path) as f:
            dct = json.load(f)
            for _, val in self.url_search_iter(dct):
                match = re.match(self.base_url_re, val)
                if match:  # it starts with {{ base_url }}
                    extra_url = match.group(2)
                    data_path = path / "data" / extra_url
                    if not data_path.exists():
                        rslt.append(f"Expected data file {Path('data') / extra_url} is absent.")

        return rslt
