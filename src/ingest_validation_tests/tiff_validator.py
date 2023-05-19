from typing import List

import tifffile
from ingest_validation_tools.plugin_validator import Validator

class TiffValidator(Validator):
    description = "Recursively test all tiff files that are not ome.tiffs for validity"
    cost = 1.0
    def collect_errors(self, **kwargs) -> List[str]:
        rslt = []
        for glob_expr in ['**/*.tif', '**/*.tiff', '**/*.TIFF', '**/*.TIF']:
            for path in self.path.glob(glob_expr):
                # skip ome.tiffs; they are checked elsewhere
                if path.name.lower().endswith(('.ome.tif', '.ome.tiff')):
                    continue
                try:
                    with tifffile.TiffFile(path) as tf:
                        for page in tf.pages:
                            _ = page.shape
                            _ = page.dtype
                except:
                    rslt.append(f'{path} is not a valid TIFF file')
        return rslt
