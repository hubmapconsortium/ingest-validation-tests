"""
Test for some common errors in the directory and file structure of CODEX datasets.
"""

from typing import List

import pandas as pd
from ingest_validation_tools.plugin_validator import Validator


class QuitNowException(Exception):
    """
    Signal exit from this validation test
    """
    pass


def _split_cycle_dir_string(cycle_str):
    """
    Given a cycle-and-region directory name, split out the cyle and region numbers
    """
    words = cycle_str.split('_')
    assert len(words) >= 2, f'Directory string "{cycle_str}" has unexpected form'
    assert words[0].startswith('cyc'), (f'directory string "{cycle_str}" does'
                                        ' not start with "cyc"')
    try:
        cyc_id = int(words[0][len('cyc'):])
    except ValueError:
        raise AssertionError(f'Directory string "{cycle_str}" cycle number is'
                             ' not an integer')
    assert words[1].startswith('reg'), (f'Directory string "{cycle_str}" does'
                                        ' not include "_reg"')
    try:
        reg_id = int(words[1][len('reg'):])
    except ValueError:
        raise AssertionError(f'Directory string "{cycle_str}" region number is'
                             ' not an integer')
    return cyc_id, reg_id


class CodexCommonErrorsValidator(Validator):
    """
    Test for some common errors in the directory and file structure of
    CODEX datasets.
    """
    description = "Test for common problems found in CODEX"
    cost = 1.0
    required = "codex"

    def collect_errors(self, **kwargs) -> List[str]:
        """
        Return the errors found by this validator
        """
        if (
            self.required not in self.contains
            and self.assay_type.lower() != self.required
        ):
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

            # Does dataset.json exist?  If so, 'new CODEX' syntax rules
            # are in effect
            dataset_json_exists = False
            any_dataset_json_exists = False
            for candidate in self.path.glob('**/dataset.json'):
                any_dataset_json_exists = True
                if candidate == prefix / 'dataset.json':
                    dataset_json_exists = True
            if dataset_json_exists:
                print('FOUND dataset.json; skipping further analysis')
                raise QuitNowException()
            elif any_dataset_json_exists:
                rslt.append('A dataset.json file exists but'
                            ' is in the wrong place')

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

            # Does the channelnames.txt file exist?
            channelnames_txt_path = prefix / 'channelnames.txt'
            if not channelnames_txt_path.is_file():
                # sometimes we see this variant
                channelnames_txt_path = prefix / 'channelNames.txt'
                if not channelnames_txt_path.is_file():
                    rslt.append('channelnames.txt is missing')
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

            # Does the channelnames_report.csv file exist?
            report_csv_path = prefix / 'channelnames_report.csv'
            if report_csv_path.is_file():
                # Parse channelnames_report.txt into a dataframe
                try:
                    rpt_df = pd.read_csv(str(report_csv_path), sep=',', header=None)
                except Exception:
                    rslt.append(f'Unexpected error reading {report_csv_path}')
                    raise QuitNowException()
                if len(rpt_df) == len(cn_df) + 1:
                    # channelnames_report.csv appears to have a header
                    try:
                        rpt_df = pd.read_csv(str(report_csv_path), sep=',')
                    except Exception:
                        rslt.append(f'Unexpected error reading {report_csv_path}')
                        raise QuitNowException()
                if len(rpt_df.columns) != 2:
                    rslt.append(f'Could not parse {report_csv_path}.'
                                ' Is it a comma-separated table?'
                    )
                    raise QuitNowException()
                col_0, col_1 = rpt_df.columns
                rpt_df = rpt_df.rename(columns={col_0:'Marker', col_1:'Result'})
                # Do they match?
                rpt_df['other'] = cn_df[0]
                mismatches_df = rpt_df[rpt_df['other'] != rpt_df['Marker']]
                if len(mismatches_df) != 0:
                    for idx, row in mismatches_df.iterrows():
                        rslt.append(
                            f"{channelnames_txt_path.name} does not"
                            " match channelnames_report.txt"
                            f" on line {idx}: {row['other']} vs {row['Marker']}"
                        )
                    raise QuitNowException()
            else:
                rpt_df = None

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
            # Total number of channels / total number of cycles must be integer,
            # excluding any HandE channels
            total_channel_count = len(cn_df)
            h_and_e_channel_count = len(cn_df[cn_df[0].str.startswith('HandE')])
            channels_per_cycle = ((total_channel_count - h_and_e_channel_count)
                                  / len(cycles))
            if channels_per_cycle != int(channels_per_cycle):
                failures.append('The number of channels per cycle is not constant')
            if failures:
                rslt += failures
                raise QuitNowException()

        except QuitNowException:
            pass
        return rslt
