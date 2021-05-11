from typing import List

import xmlschema
import tifffile
from ingest_validation_tools.plugin_validator import Validator

class OmeTiffValidator(Validator):
    description = "Recursively test all ome-tiff files for validity"
    cost = 1.0
    def collect_errors(self) -> List[str]:
        rslt = []
        for glob_expr in ['**/*.ome.tiff', '**/*.OME.TIFF']:
            for path in self.path.glob(glob_expr):
                try:
                    with tifffile.TiffFile(path) as tf:
                        xml_document = xmlschema.XmlDocument(tf.ome_metadata)
                    if not xml_document.schema.is_valid(xml_document):
                        rslt.append(f'{path} is not a valid OME.TIFF file')
                except:
                    rslt.append(f'{path} is not a valid OME.TIFF file')
        return rslt


