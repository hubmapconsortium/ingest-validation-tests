from pathlib import Path, PosixPath
from xml.etree.ElementTree import ParseError

import pytest
from validator import (
    FileTypes,
    Validator,
    check_ome_xml,
    find_all_files,
    find_files,
    verify_filename,
    verify_qptiff_filename,
)


class ValidatorTestClass(Validator):
    version = "1.0"
    required = ["required_type"]

    def __init__(
        self,
        base_paths,
        assay_type,
        rslt: list[str] = [],
        data_tested: list[str] = [],
        contains=[],
        schema=None,
        app_context={},
        coreuse=None,
    ):
        super().__init__(
            base_paths,
            assay_type,
            contains=contains,
            verbose=True,
            schema=schema,
            globus_token="",
            app_context=app_context,
            coreuse=coreuse,
        )
        self.rslt = rslt
        self.data_tested = data_tested

    def _collect_errors(self):
        return self._return_result(self.rslt, self.data_tested)


default_kwargs = {"rslt": ["results"], "data_tested": ["buncha_paths"]}


def test_required_match():
    v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
    assert v.collect_errors() == default_kwargs["rslt"]


def test_required_no_match():
    v = ValidatorTestClass(["tmp_path"], "not_required_type", **default_kwargs)
    assert v.collect_errors() == []


def test_multiple_required_match():
    v = ValidatorTestClass(["tmp_path"], "assay_type2", **default_kwargs)
    v.required = ["assay_type1", "assay_type2"]
    assert v.collect_errors() == default_kwargs["rslt"]


def test_multiple_required_no_match():
    v = ValidatorTestClass(["tmp_path"], "assay_type3", **default_kwargs)
    v.required = ["assay_type1", "assay_type2"]
    assert v.collect_errors() == []


def test_contains_match():
    v = ValidatorTestClass(
        ["tmp_path"], "not_required_type", contains=["required_type"], **default_kwargs
    )
    assert v.collect_errors() == default_kwargs["rslt"]


def test_contains_no_match():
    v = ValidatorTestClass(
        ["tmp_path"], "not_required_type1", contains=["not_required_type2"], **default_kwargs
    )
    assert v.collect_errors() == []


def test_contains_multiple_match():
    v = ValidatorTestClass(
        ["tmp_path"],
        "not_required_type",
        contains=["required_type", "other_required_type"],
        **default_kwargs
    )
    assert v.collect_errors() == default_kwargs["rslt"]


def test_contains_multiple_no_match():
    v = ValidatorTestClass(
        ["tmp_path"],
        "not_required_type1",
        contains=["not_required_type2", "not_required_type3"],
        **default_kwargs
    )
    assert v.collect_errors() == []


def test_return_result_errors():
    v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
    assert v.collect_errors() == default_kwargs["rslt"]


def test_return_result_no_errors():
    v = ValidatorTestClass(
        ["tmp_path"], "required_type", **{"rslt": [], "data_tested": ["whatever"]}
    )
    assert v.collect_errors() == [None]


def test_return_result_not_run_bad_type():
    v = ValidatorTestClass(
        ["tmp_path"], "not_required_type", **{"rslt": [], "data_tested": ["whatever"]}
    )
    assert v.collect_errors() == []


def test_return_result_not_run_no_data():
    v = ValidatorTestClass(["tmp_path"], "not_required_type", **{"rslt": [], "data_tested": []})
    assert v.collect_errors() == []


def test_threads_core_use_param():
    v = ValidatorTestClass(["tmp_path"], "required_type", coreuse=4, **default_kwargs)
    assert v.threads == 4


def test_threads_cpu_count_calc_gt_1(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("validator.cpu_count", lambda: 8)
        v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
        assert v.threads == 2


def test_threads_cpu_count_calc_lt_1(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("validator.cpu_count", lambda: 3)
        v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
        assert v.threads == 1


def test_threads_default_to_1(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("validator.cpu_count", lambda: 0)
        v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
        assert v.threads == 1


def create_bad_files(dir_path: Path, expected_dir: bool = True):
    if not dir_path.exists():
        Path(dir_path).mkdir(parents=True)
    bad_files = [  # exclude bad suffix
        PosixPath(dir_path / "test.qptiff.raw"),
        # exclude .raw.qptiff
        PosixPath(dir_path / "test.raw.qptiff"),
        # exclude .intermediate.qptiff
        PosixPath(dir_path / "test.intermediate.qptiff"),
    ]
    if expected_dir:
        bad_files.extend(
            [  # exclude valid file in subdir if expected dir is found
                PosixPath(dir_path / "test_dir/test.qptiff"),
                # exclude valid file in different dir if expected dir is found
                PosixPath(dir_path / "test.qptiff"),
            ]
        )
        Path(dir_path / "test_dir").mkdir(parents=True)
    for file in bad_files:
        write_file(file)
    return bad_files


def create_good_files(
    dir_path: Path, expected_dir: bool = True, additional_paths: list[Path] = []
):
    if not dir_path.exists():
        Path(dir_path).mkdir(parents=True)
    good_files = [
        PosixPath(dir_path / "extras.qptiff"),
        PosixPath(dir_path / "test.qptiff"),
    ] + additional_paths
    if not expected_dir:
        good_files.append(PosixPath(dir_path / "test_dir/test.qptiff"))
        Path(dir_path / "test_dir").mkdir(parents=True)
    for path in additional_paths:
        if not path.parent.exists():
            Path(path.parent).mkdir(parents=True)
    for file in good_files:
        write_file(file)
    return good_files


def write_file(file):
    with open(file, "w", newline="") as mock_file:
        mock_file.write("should be ignored")


def test_qptifffinder_good_expected_dir(tmp_path):
    expected_dir_path = Path(tmp_path / "raw/images")
    good_filenames = create_good_files(expected_dir_path)
    create_bad_files(expected_dir_path)
    assert sorted(find_files(tmp_path, FileTypes.QPTIFF)) == sorted(good_filenames)


def test_qptifffinder_no_expected_dir(tmp_path):
    good_filenames = create_good_files(tmp_path, expected_dir=False)
    create_bad_files(tmp_path, expected_dir=False)
    assert sorted(find_files(tmp_path, FileTypes.QPTIFF)) == sorted(good_filenames)


def test_qptifffinder_no_expected_dir_restricted(tmp_path):
    assert sorted(find_files(tmp_path, FileTypes.QPTIFF, restrict_to_expected=True)) == []


def test_qptifffinder_find_all(tmp_path):
    good_filenames = create_good_files(
        tmp_path, expected_dir=False, additional_paths=[Path(tmp_path / "raw/images/test.qptiff")]
    )
    create_bad_files(tmp_path, expected_dir=False)
    assert sorted(find_all_files(tmp_path, FileTypes.QPTIFF)) == sorted(good_filenames)


def test_qptifffinder_valid_filenames():
    files = [
        ("S09_TMA07_030926_Scan1.qptiff", True),
        ("S09_TMA07_030926_Scan1_Cycle16.raw.qptiff", False),
        ("S09_TMA07_030926_Scan1_Cycle13.intermediate.qptiff", False),
        ("S09_TMA07_030926_Scan1_Cycle19.raw.qptiff", False),
        ("bad_file", False),
        ("extra.qptiff", True),
        ("extras/good.qptiff", False),
    ]
    for file, rslt in files:
        assert verify_filename(file, FileTypes.QPTIFF) == rslt
        assert verify_qptiff_filename(file) == rslt


def test_invalid_ome_xml():
    bad_ome_xml = "\x00\x00\x00\x00etadata><Key>Image_H001_DUO_01A.vsi #22 Value #151</Key><Value>-24.699999999999996</Value></OriginalMetadata></Value></XMLAnnotation></StructuredAnnotations></OME>"
    with pytest.raises(Exception) as e:
        check_ome_xml(bad_ome_xml, "")
        assert "Error parsing" in str(e)
