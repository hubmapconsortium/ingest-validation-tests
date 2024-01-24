from typing import List

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator


class OmeTiffValidator(Validator):
    description = "Recursively test all ome-tiff files for validity"
    cost = 1.0

    def collect_errors(self, **kwargs) -> List[str]:
        del kwargs
        rslt = []
        for glob_expr in ['**/*.ome.tif', '**/*.ome.tiff', '**/*.OME.TIFF', '**/*.OME.TIF']:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    try:
                        with tifffile.TiffFile(file) as tf:
                            xml_document = xmlschema.XmlDocument(tf.ome_metadata)
                        if not xml_document.schema.is_valid(xml_document):
                            rslt.append(f'{file} is not a valid OME.TIFF file')
                    except Exception as excp:
                        rslt.append(f'{file} is not a valid OME.TIFF file: {excp}')
        return rslt
