from multiprocessing import Pool
from os import cpu_count
from typing import List

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator


def _check_ome_tiff_file(file: str):
    try:
        with tifffile.TiffFile(file) as tf:
            xml_document = xmlschema.XmlDocument(tf.ome_metadata)
        if xml_document.schema and not xml_document.schema.is_valid(xml_document):
            return f"{file} is not a valid OME.TIFF file"
    except Exception as excp:
        return f"{file} is not a valid OME.TIFF file: {excp}"


class OmeTiffValidator(Validator):
    description = "Recursively test all ome-tiff files for validity"
    cost = 1.0
    version = "1.0"

    def collect_errors(self, **kwargs) -> List[str]:
        threads = kwargs.get("coreuse", None) or cpu_count() // 4 or 1
        pool = Pool(threads)
        filenames_to_test = []
        for glob_expr in [
            "**/*.ome.tif",
            "**/*.ome.tiff",
            "**/*.OME.TIFF",
            "**/*.OME.TIF",
        ]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)

        rslt_list = list(
            rslt
            for rslt in pool.imap_unordered(_check_ome_tiff_file, filenames_to_test)
            if rslt is not None
        )
        if rslt_list:
            return rslt_list
        elif filenames_to_test:
            return [None]
        else:
            return []
