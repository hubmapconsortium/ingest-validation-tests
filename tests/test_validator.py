import os

from validator import Validator


class ValidatorTestClass(Validator):
    version = "1.0"
    required = ["required_type"]

    def __init__(
        self,
        base_paths,
        assay_type,
        contains=[],
        verbose=True,
        schema=None,
        globus_token="",
        app_context={},
        coreuse=None,
        **kwargs
    ):
        super().__init__(
            base_paths,
            assay_type,
            contains=contains,
            verbose=verbose,
            schema=schema,
            globus_token=globus_token,
            app_context=app_context,
            coreuse=coreuse,
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


def test_threads_calculation_over_1(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("validator.cpu_count", lambda: 8)
        v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
        assert v.threads == 2


def test_threads_calculation_under_1(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("validator.cpu_count", lambda: 3)
        v = ValidatorTestClass(["tmp_path"], "required_type", **default_kwargs)
        assert v.threads == 1
