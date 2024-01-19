from typing import List
from pathlib import Path

import tifffile
import xmlschema
from ingest_validation_tools.plugin_validator import Validator


# def _check_ome_tiff_file(paths: str):
#     rslt_list = []
#     for path in paths:
#         try:
#             with tifffile.TiffFile(path) as tf:
#                 xml_document = xmlschema.XmlDocument(tf.ome_metadata)
#             if not xml_document.schema.is_valid(xml_document):
#                 return f"{path} is not a valid OME.TIFF file"
#         except Exception as excp:
#             rslt_list.append(f"{path} is not a valid OME.TIFF file: {excp}")
#     return rslt_list
#
# def _glob_paths(path: str):
#     filenames_to_test = []
#     for glob_expr in ['**/*.ome.tif', '**/*.ome.tiff', '**/*.OME.TIFF', '**/*.OME.TIF']:
#         for file in path.glob(glob_expr):
#             filenames_to_test.append(file)
#     return filenames_to_test
#
#
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
    # def collect_errors(self, **kwargs) -> List[str]:
    #     del kwargs
    #     filenames_to_test = list(
    #         file
    #         for file in self.pool.imap_unordered(_glob_paths, self.paths)
    #         if file is not None
    #     )
    #     # TODO: this is awful
    #     return list(
    #         x
    #         for y in list(
    #             rslt
    #             for rslt in self.pool.imap_unordered(
    #                 _check_ome_tiff_file, filenames_to_test
    #             )
    #             if rslt is not None
    #         )
    #         for x in y
    #     )
