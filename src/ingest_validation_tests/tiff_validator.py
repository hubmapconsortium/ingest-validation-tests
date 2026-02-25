from multiprocessing import Pool

import tifffile
from validator import Validator


def _check_tiff_file(path: str) -> str | None:
    try:
        with tifffile.TiffFile(path) as tfile:
            for page in tfile.pages:
                _ = page.asarray()  # force decompression
        return None
    except Exception as excp:
        print(f"{path} is not a valid TIFF file: {excp}")
        return f"{path} is not a valid TIFF file: {excp}"


class TiffValidator(Validator):
    description = "Recursively test all tiff files (including ome.tiffs) for validity"
    cost = 1.0
    version = "1.0"

    def _collect_errors(self, **kwargs) -> list[str | None]:
        del kwargs
        pool = Pool(self.threads)
        filenames_to_test = []
        for glob_expr in [
            "**/*.[tT][iI][fF]",
            "**/*.[tT][iI][fF][fF]",
        ]:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)
        try:
            rslt_list: list[str | None] = list(
                rslt
                for rslt in pool.imap_unordered(_check_tiff_file, filenames_to_test)
                if rslt is not None
            )
        except Exception as e:
            self._log(f"Error {e}")
            rslt_list = [f"Error {e}"]
        pool.close()
        pool.join()
        return self._return_result(rslt_list, filenames_to_test)
