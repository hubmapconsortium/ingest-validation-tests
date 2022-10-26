from typing import List
from pathlib import Path

import pandas as pd
from ingest_validation_tools.plugin_validator import Validator


class QuitNowException(Exception):
    pass


def _split_cycle_dir_string(s):
    words = s.split('_')
    assert len(words) >= 2, f'Directory string "{s}" has unexpected form'
    assert words[0].startswith('cyc'), (f'directory string "{s}" does'
                                        ' not start with "cyc"')
    try:
        cyc_id = int(words[0][len('cyc'):])
    except ValueError:
        raise AssertionError(f'Directory string "{s}" cycle number is'
                             ' not an integer')
    assert words[1].startswith('reg'), (f'Directory string "{s}" does'
                                        ' not include "_reg"')
    try:
        reg_id = int(words[1][len('reg'):])
    except ValueError:
        raise AssertionError(f'Directory string "{s}" region number is'
                             ' not an integer')
    return cyc_id, reg_id
    

class CodexCommonErrorsValidator(Validator):
    description = "Test for common problems found in CODEX"
    cost = 1.0
    def collect_errors(self, **kwargs) -> List[str]:
        if self.assay_type != 'CODEX':
            return []  # We only test CODEX data
        rslt = []
        try:
            # is the raw/src_ directory present?
            prefix = None
            if (self.path / 'raw').is_dir():
                prefix = self.path / 'raw'
            else:
                for candidate in self.path.glob('src_*'):
                    prefix = candidate
            if prefix is None:
                rslt.append('The raw/src_ subdirectory is missing?')
                raise QuitNowException()
            
            # is the segmentation.json file on the right side?
            found = False
            right_place = False
            for path in self.path.glob('*/[Ss]egmentation.json'):
                rel_path = path.relative_to(self.path)
                found = True
                if str(rel_path).startswith(('raw', 'src_')):
                    right_place = True
            if found:
                if right_place:
                    pass
                else:
                    rslt.append('The segmentation.json file is in the wrong subdirectory')
            else:
                rslt.append('The segmentation.json file is missing or misplaced')

            # Do the channelnames.txt and channelnames_report.csv files exist?
            channelnames_txt_path = prefix / 'channelnames.txt'
            report_csv_path = prefix / 'channelnames_report.csv'
            if not (channelnames_txt_path.is_file() and report_csv_path.is_file()):
                rslt.append('channelnames.txt and/or channelnames_report.csv is missing')
                raise QuitNowException()

            # Parse channelnames.txt into a dataframe
            try:
                cn_df = pd.read_csv(str(channelnames_txt_path), header=None)
            except Exception:
                rslt.append(f'Unexpected error reading {channelnames_txt_path}')
                raise QuitNowException()
            if len(cn_df.columns) != 1:
                rslt.append(f'Unexpected format for {channelnames_txt_path}')
                raise QuitNowException()

            # Parse channelnames_report.txt into a dataframe
            try:
                rpt_df = pd.read_csv(str(report_csv_path), sep=',')
            except Exception:
                rslt.append(f'Unexpected error reading {report_csv_path}')
                raise QuitNowException()
            if (len(rpt_df.columns) != 2
                or rpt_df.columns[0] != 'Marker'
                or rpt_df.columns[1] != 'Result'
                ):
                rslt.append(f'Could not parse {report_csv_path}.'
                            ' Is it a comma-separated table?'
                            )
                raise QuitNowException()

            # Do they match?
            rpt_df['other'] = cn_df[0]
            mismatches_df = rpt_df[rpt_df['other'] != rpt_df['Marker']]
            if len(mismatches_df):
                for idx, row in mismatches_df.iterrows():
                    rslt.append(
                        "channelnames.txt does not match channelnames_report.txt"
                        f" on line {idx}: {row['other']} vs {row['Marker']}"
                    )
                raise QuitNowException()
                    
            # Tabulate the cycle and region info
            all_cycle_dirs = []
            for glob_str in ['cyc*', 'Cyc*']:
                for pth in prefix.glob(glob_str):
                    if pth.is_dir():
                        all_cycle_dirs.append(str(pth.stem).lower())
            cycles = []
            regions = []
            failures = []
            for cyc_dir in all_cycle_dirs:
                try:
                    cyc_id, reg_id = _split_cycle_dir_string(cyc_dir)
                    cycles.append(cyc_id)
                    regions.append(reg_id)
                except AssertionError as excp:
                    failures.append(str(excp))
            if failures:
                rslt += failures
                raise QuitNowException()
            total_entries = len(cycles)
            cycles = list(set(cycles))
            cycles.sort()
            regions = list(set(regions))
            regions.sort()
            failures = []
            # First cycle must be 1
            if cycles[0] != 1:
                failures.append('Cycle numbering does not start at 1')
            # First region must be 1
            if regions[0] != 1:
                failures.append('Region numbering does not start at 1')
            # Cycle range must be contiguous ints
            if cycles != list(range(cycles[0], cycles[-1]+1)):
                failures.append('Cycle numbers are not contiguous')
            # Region range must be contiguous ints
            if regions != list(range(regions[0], regions[-1]+1)):
                failures.append('Region numbers are not contiguous')
            # All cycle, region pairs must be present
            if len(cycles) * len(regions) != total_entries:
                failures.append('Not all cycle/region pairs are present')
            # Total number of channels / total number of cycles must be integer
            channels_per_cycle = len(rpt_df) / len(cycles)
            if channels_per_cycle != int(channels_per_cycle):
                failures.append('The number of channels per cycle is not constant')
            if failures:
                rslt += failures
                raise QuitNowException()
            
        except QuitNowException:
            pass
        return rslt


