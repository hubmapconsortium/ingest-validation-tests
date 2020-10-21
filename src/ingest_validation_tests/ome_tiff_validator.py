import xmltodict
import tifffile
from ingest_validation_tools.plugin_validator import Validator

class OmeTiffValidator(Validator):
    description = "Recursively test all ome-tiff files for validity"
    cost = 1.0
    def collect_errors(self) -> List[str]:
        rslt = []
        for path in self.path.glob('**/*.ome.tiff') + self.path.glob('**/*.OME.TIFF'):
            try:
                with tifffile.TiffFile(path) as tf:
                    metadata = xmltodict.parse(tf.ome_metadata)  # @Notused
            except:
                rslt.append(f'{path} is not a valid PNG file')
                raise
        return rslt

