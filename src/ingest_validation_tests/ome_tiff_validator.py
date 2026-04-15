from multiprocessing import Pool

from validator import Validator, check_ome_tiff_file, ome_tiff_globs


def _check_ome_tiff_file(file):
    try:
        check_ome_tiff_file(file)
    except Exception as e:
        return str(e)


class OmeTiffValidator(Validator):
    description = "Recursively test all ome-tiff files for validity"
    cost = 1.0
    version = "1.0"

    def _collect_errors(self) -> list[str | None]:
        pool = Pool(self.threads)
        filenames_to_test = []
        for glob_expr in ome_tiff_globs:
            for path in self.paths:
                for file in path.glob(glob_expr):
                    filenames_to_test.append(file)

        rslt_list: list[str | None] = list(
            rslt
            for rslt in pool.imap_unordered(_check_ome_tiff_file, filenames_to_test)
            if rslt is not None
        )
        pool.close()
        pool.join()
        return self._return_result(rslt_list, filenames_to_test)
