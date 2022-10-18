from typing import List
from pathlib import Path

import pandas as pd
from ingest_validation_tools.plugin_validator import Validator


class QuitNowException(Exception):
    pass


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
                #print(f'path {rel_path}')
                found = True
                if str(rel_path).startswith(('raw','src_')):
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
        except QuitNowException:
            return rslt
        return rslt


