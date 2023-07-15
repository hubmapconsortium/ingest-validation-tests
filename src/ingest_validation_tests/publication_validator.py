"""
Test for some common errors in the directory and file structure of publications.
"""

from typing import List
import re
import json
from pathlib import Path
import frontmatter
from ingest_validation_tools.plugin_validator import Validator


class PublicationValidator(Validator):
    """
    Test for some common errors in the directory and file structure of
    publications.
    """
    description = "Test for common problems found in publications"
    cost = 1.0
    base_url_re = r'(\s*\{\{\s*base_url\s*\}\})/(.*)'
    def collect_errors(self, **kwargs) -> List[str]:
        """
        Return the errors found by this validator
        """
        if self.assay_type != 'Publication':
            return []  # We only test Publication data
        rslt = []
        try:
            vignette_path = self.path / 'vignettes'
            assert vignette_path.is_dir(), 'vignettes not found or not a directory'
            for this_vignette_path in vignette_path.glob('*'):
                assert this_vignette_path.is_dir(), (f"Found the non-dir {this_vignette_path}"
                                                     " in vignettes")
                this_vignette_all_paths = set(this_vignette_path.glob('*'))
                if not all(pth.is_file() for pth in this_vignette_all_paths):
                    raise AssertionError('Found a subdirectory in a vignette')
                md_found = False
                vig_figures = []
                for md_path in this_vignette_path.glob('*.md'):
                    if md_found:
                        raise AssertionError('A vignette has more than one markdown file')
                    else:
                        md_found = True
                    vig_fm = frontmatter.loads(md_path.read_text())
                    for key in ['name', 'figures']:
                        assert key in vig_fm.metadata, f'vignette markdown has no {key}'
                    for fig_dict in vig_fm.metadata['figures']:
                        assert 'file' in fig_dict, 'figure dict does not reference a file'
                        assert 'name' in fig_dict, 'figure dict does not provide a name'
                        vig_figures.append(fig_dict['file'])
                    this_vignette_all_paths.remove(md_path)
                    for fname in vig_figures:
                        rslt.extend(self.validate_vitessce_config(this_vignette_path / fname))
                        this_vignette_all_paths.remove(this_vignette_path / fname)
                assert not this_vignette_all_paths, ('unexpected files in vignette:'
                                                     f' {list(str(elt) for elt in this_vignette_all_paths)}')

        except AssertionError as excp:
            rslt.append(str(excp))

        return rslt

    def validate_vitessce_config(self, json_path):
        rslt = []
        with open(json_path) as f:
            dct = json.load(f)
        dataset_list = dct.get('datasets', [])
        for dataset in dataset_list:
            fblock_list = dataset.get('files', [])
            for fblock in fblock_list:
                try:
                    assert 'url' in fblock, (f"json at {json_path}"
                                             " references a dataset with no url")
                    match = re.match(self.base_url_re, fblock['url'])
                    if match:  # it starts with {{ base_url }}
                        extra_url = match.group(2)
                        data_path = self.path / 'data' / extra_url
                        assert data_path.exists(), ("expected data file"
                                                    f" {Path('data') / extra_url} is absent")
                except AssertionError as excp:
                    rslt.append(str(excp))
        return rslt
