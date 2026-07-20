import json
from functools import cached_property
from pathlib import Path

import tifffile
from shapely.geometry import box, shape
from validator import (
    FileTypes,
    Validator,
    find_files,
    get_non_global_paths_by_row,
    verify_ome_tiff_filename,
    verify_qptiff_filename,
)


class GeoJsonTiffValidator(Validator):
    description = "Confirm GeoJSON tissue boundaries intersect TIFF/QPTIFF pixel dimensions"
    cost = 1.0
    version = "1.0"

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.shared_upload = any(
            bool(path.stem in ["global", "non_global"]) for path in self.paths
        )
        self.errors = []

    def _collect_errors(self) -> list[str | None]:
        if not self.files_to_test:
            return []
        try:
            self._run_validation()
        except Exception as e:
            self.errors.append(f"Error during validation: {e}")
        finally:
            return self._return_result(self.errors, bool(self.files_to_test))

    def _run_validation(self):
        for files_dict in self.files_to_test.values():
            tiff_files = files_dict["ome_tiff"] if files_dict["ome_tiff"] else files_dict["qptiff"]
            self.errors.extend(
                self._check_tiff_counts(files_dict["ome_tiff"], files_dict["qptiff"])
            )
            result = self._check_geojson_intersects_tiffs(files_dict["geojson"][0], tiff_files)
            if result is not None:
                self.errors.append(result)

    ####################
    # File collection  #
    ####################

    @cached_property
    def files_to_test(self) -> dict[int, dict[str, list[Path]]]:
        """
        For each data path, locate a single GeoJSON file and TIFF files.
        Returns:
            {<data_path>: {
                "geojson": [<geojson_path>],
                "ome_tiff": [<ome_tiff_paths>],
                "qptiff": [<qptiff_paths>]}}
        """
        files_to_test = {}
        for path in self.paths:
            if self.shared_upload:
                files_to_test.update(self._get_shared_upload_file_pairs(path.parent))
                break
            geojson_file = self._get_geojson_file(path)
            if geojson_file is None:
                continue
            ome_tiffs = find_files(path, FileTypes.OME_TIFF)
            qptiffs = find_files(path, FileTypes.QPTIFF)
            files_to_test[path] = {
                "geojson": [geojson_file],
                "ome_tiff": ome_tiffs,
                "qptiff": qptiffs,
            }
        return files_to_test

    def _get_geojson_file(self, path: Path) -> Path | None:
        files = list(path.glob("**/*.geojson"))
        if len(files) > 1:
            names = ", ".join(f.name for f in files)
            self.errors.append(
                f"Found {len(files)} GeoJSON files in {self.rel_filename_str(path)} (expected 1): {names}"
            )
            return None
        return files[0] if files else None

    def _get_shared_upload_file_pairs(self, base_path: Path) -> dict[int, dict[str, list[Path]]]:
        """
        non_global directory may contain GeoJSON and TIFF files per dataset row.
        Read non_global_files field of metadata.tsv and retrieve files from there.
        Fill TIFF files from global/ when not found in non_global.
        """
        if not self.schema_rows:
            raise Exception("Row data from metadata.tsv is required to validate shared uploads.")
        try:
            non_global_paths = get_non_global_paths_by_row(self.schema_rows, base_path)
        except Exception as e:
            self.errors.append(str(e))
            return {}

        global_path = base_path / "global"
        global_ome_tiffs: list[Path] = []
        global_qptiffs: list[Path] = []
        if global_path.exists():
            global_ome_tiffs = find_files(global_path, FileTypes.OME_TIFF)
            global_qptiffs = find_files(global_path, FileTypes.QPTIFF)
        all_files: dict[int, dict[str, list[Path]]] = {}
        for row_num, row_paths in non_global_paths.items():
            geojson_files = [p for p in row_paths if p.suffix.lower() == ".geojson"]
            if not geojson_files:
                continue
            if len(geojson_files) > 1:
                names = ", ".join(p.name for p in geojson_files)
                self.errors.append(
                    f"Found {len(geojson_files)} GeoJSON files for dataset row {row_num} (expected 1): {names}"
                )
                continue
            ome_tiffs = list(
                set([p for p in row_paths if verify_ome_tiff_filename(p)] + global_ome_tiffs)
            )
            qptiffs = list(
                set([p for p in row_paths if verify_qptiff_filename(p)] + global_qptiffs)
            )
            all_files[row_num] = {
                "geojson": geojson_files,
                "ome_tiff": ome_tiffs,
                "qptiff": qptiffs,
            }
        return all_files

    ####################
    # TIFF validation  #
    ####################

    def _check_tiff_counts(self, ome_tiffs: list[Path], qptiffs: list[Path]) -> list[str]:
        errors = []
        if len(ome_tiffs) > 1:
            names = ", ".join(f.name for f in ome_tiffs)
            errors.append(
                f"GeoJSON present but found {len(ome_tiffs)} OME-TIFFs (expected 1): {names}"
            )
        elif len(ome_tiffs) == 0 and len(qptiffs) > 1:
            names = ", ".join(f.name for f in qptiffs)
            errors.append(
                f"GeoJSON present but found {len(qptiffs)} QPTIFFs and no OME-TIFFs (expected 1 QPTIFF): {names}"
            )
        return errors

    def _get_tiff_bounds(self, path: Path) -> tuple[int, int] | str:
        try:
            with tifffile.TiffFile(path) as tf:
                series = tf.series[0]  # type: ignore
                axes = series.axes.upper()
                shape_ = series.shape
                if "Y" not in axes or "X" not in axes:
                    return f"{path}: no Y/X axes found (axes={axes})"
                width = shape_[axes.index("X")]
                height = shape_[axes.index("Y")]
                return (width, height)
        except Exception as e:
            return f"{path.stem}: could not read dimensions: {e}"

    def _check_geojson_intersects_tiffs(
        self, geojson_path: Path, tiff_paths: list[Path]
    ) -> str | None:
        try:
            with open(geojson_path) as f:
                gj = json.load(f)
        except Exception as e:
            return f"{geojson_path}: could not parse GeoJSON: {e}"

        if gj.get("type") == "Feature":
            features = [gj]
        else:
            features = gj.get("features", [])
        if not features:
            return f"{geojson_path}: no features found"

        tiff_dims = []
        for tp in tiff_paths:
            result = self._get_tiff_bounds(tp)
            if not isinstance(result, str):
                tiff_dims.append((tp, result))
            else:
                self.errors.append(result)
        if not tiff_dims:
            return None  # no readable TIFFs, skip

        for feature in features:
            try:
                geom = shape(feature["geometry"])
            except Exception as e:
                return f"{geojson_path}: invalid geometry: {e}"
            for _, (width, height) in tiff_dims:
                tiff_box = box(0, 0, width, height)
                if tiff_box.intersects(geom):
                    return None  # at least one intersection found

        tiff_dims_str = "; ".join(f"{tp.name} ({w}x{h})" for tp, (w, h) in tiff_dims)
        return (
            f"{geojson_path.name}: GeoJSON boundaries do not intersect any TIFF "
            f"pixel dimensions ({tiff_dims_str})"
        )
