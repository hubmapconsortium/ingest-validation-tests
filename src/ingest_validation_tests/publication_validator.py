"""
Test for some common errors in the directory and file structure of publications.
"""

from typing import List
import frontmatter
from ingest_validation_tools.plugin_validator import Validator


class PublicationValidator(Validator):
    """
    Test for some common errors in the directory and file structure of
    publications.
    """
    description = "Test for common problems found in publications"
    cost = 1.0
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
                        this_vignette_all_paths.remove(this_vignette_path / fname)
                assert not this_vignette_all_paths, ('unexpected files in vignette:'
                                                     f' {list(str(elt) for elt in this_vignette_all_paths)}')

        except AssertionError as excp:
            rslt.append(str(excp))
                                          
        return rslt
