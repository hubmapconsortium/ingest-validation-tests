from pathlib import Path, PosixPath

from validator import QptiffFinder, Validator


class ValidatorTestClass(Validator):
    version = "1.0"
    required = ["required_type"]

    def __init__(self, base_paths, assay_type, contains=[], **kwargs):
        super().__init__(
            base_paths,
            assay_type,
            contains=contains,
            verbose=True,
            schema=None,
            globus_token="",
            app_context={},
            coreuse=None,
        )
        self.rslt = kwargs.get("rslt")
        self.data_tested = kwargs.get("data_tested")

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


def create_bad_filenames(dir_path: Path, expected_dir: bool = True):
    bad_dirs = [  # exclude bad suffix
        PosixPath(dir_path / "test.qptiff.raw"),
        # exclude .raw.qptiff
        PosixPath(dir_path / "test.raw.qptiff"),
        # exclude .intermediate.qptiff
        PosixPath(dir_path / "test.intermediate.qptiff"),
    ]
    if expected_dir:
        bad_dirs.extend(
            [  # exclude valid file in subdir if expected dir is found
                PosixPath(dir_path / "test_dir/test.qptiff"),
                # exclude valid file in different dir if expected dir is found
                PosixPath(dir_path / "test.qptiff"),
            ]
        )
    return bad_dirs


def create_good_filenames(dir_path: Path, additional_paths: list[Path] = []):
    return [
        PosixPath(dir_path / "extras.qptiff"),
        PosixPath(dir_path / "test.qptiff"),
    ] + additional_paths


def test_qptifffinder_good_expected_dir(tmp_path):
    expected_dir_path = Path(tmp_path / "raw/images")
    good_filenames = create_good_filenames(expected_dir_path)
    bad_filenames = create_bad_filenames(expected_dir_path)
    Path(expected_dir_path).mkdir(parents=True)
    Path(tmp_path / "raw/images/test_dir").mkdir(parents=True)
    Path(tmp_path / "test_dir").mkdir(parents=True)
    for file in [*good_filenames, *bad_filenames]:
        with open(file, "w", newline="") as mock_file:
            mock_file.write("should be ignored")
    assert sorted(QptiffFinder(tmp_path).find()) == good_filenames


def test_qptifffinder_no_expected_dir(tmp_path):
    good_filenames = create_good_filenames(
        tmp_path,
        # should look in subdirs
        additional_paths=[PosixPath(tmp_path / "test_dir/test.qptiff")],
    )
    extras = [PosixPath(tmp_path / "extras/test.qptiff")]
    bad_filenames = create_bad_filenames(tmp_path, expected_dir=False) + extras
    Path(tmp_path / "test_dir").mkdir(parents=True)
    Path(tmp_path / "extras").mkdir(parents=True)
    for file in [*good_filenames, *bad_filenames]:
        with open(file, "w", newline="") as mock_file:
            mock_file.write("should be ignored")
    assert sorted(QptiffFinder(tmp_path).find()) == good_filenames
    assert sorted(QptiffFinder(tmp_path, exclude_extras=False).find()) == sorted(
        extras + good_filenames
    )
