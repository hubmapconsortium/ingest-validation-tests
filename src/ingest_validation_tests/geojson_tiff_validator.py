import json
from pathlib import Path

import tifffile
from shapely.geometry import box, shape
from validator import Validator


tiff_globs = [
    "**/*.[tT][iI][fF]",
    "**/*.[tT][iI][fF][fF]",
    "**/*.[qQ][pP][tT][iI][fF][fF]",
]


def _get_tiff_bounds(path: Path) -> tuple[int, int] | str:
    """Return (width, height) or error string."""
    try:
        with tifffile.TiffFile(path) as tf:
            series = tf.series[0]
            axes = series.axes.upper()
            shape_ = series.shape
            if "Y" not in axes or "X" not in axes:
                return f"{path}: no Y/X axes found (axes={axes})"
            width = shape_[axes.index("X")]
            height = shape_[axes.index("Y")]
            return (width, height)
    except Exception as e:
        return f"{path}: could not read dimensions: {e}"


def _check_geojson_intersects_tiffs(
    geojson_path: Path, tiff_paths: list[Path]
) -> str | None:
    try:
        with open(geojson_path) as f:
            gj = json.load(f)
    except Exception as e:
        return f"{geojson_path}: could not parse GeoJSON: {e}"

    features = gj.get("features", [])
    if not features:
        return f"{geojson_path}: no features found"

    tiff_dims = []
    tiff_errors = []
    for tp in tiff_paths:
        result = _get_tiff_bounds(tp)
        if isinstance(result, str):
            tiff_errors.append(result)
        else:
            tiff_dims.append((tp, result))

    if not tiff_dims:
        return None  # no readable TIFFs, skip

    for feature in features:
        try:
            geom = shape(feature["geometry"])
        except Exception as e:
            return f"{geojson_path}: invalid geometry: {e}"
        for tiff_path, (width, height) in tiff_dims:
            tiff_box = box(0, 0, width, height)
            if tiff_box.intersects(geom):
                return None  # at least one intersection found

    tiff_dims_str = "; ".join(
        f"{tp.name} ({w}x{h})" for tp, (w, h) in tiff_dims
    )
    return (
        f"{geojson_path.name}: GeoJSON boundaries do not intersect any TIFF "
        f"pixel dimensions ({tiff_dims_str})"
    )


class GeoJsonTiffValidator(Validator):
    description = "Confirm GeoJSON tissue boundaries intersect TIFF/QPTIFF pixel dimensions"
    cost = 1.0
    version = "1.0"

    def _collect_errors(self) -> list[str | None]:
        geojson_files: list[Path] = []
        tiff_files: list[Path] = []

        for path in self.paths:
            for f in path.glob("**/*.geojson"):
                geojson_files.append(f)
            for glob_expr in tiff_globs:
                for f in path.glob(glob_expr):
                    tiff_files.append(f)

        if not geojson_files:
            return []

        errors = []
        for gj_path in geojson_files:
            result = _check_geojson_intersects_tiffs(gj_path, tiff_files)
            if result is not None:
                errors.append(result)

        return self._return_result(errors, geojson_files)
