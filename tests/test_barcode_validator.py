from pathlib import Path

from barcode_validator import BarcodeValidator


class TestBarcodeValidator:
    def create_file(self, dir_path: Path, contents: list[str]):
        Path(dir_path / "raw").mkdir(parents=True, exist_ok=True)
        new_file = Path(dir_path / "raw/barcodes.txt")
        with open(new_file, "a") as f:
            for line in contents:
                f.write(f"{line}\n")

    def test_good_file(self, tmp_path):
        contents = ["AGCT", "gact", "TTTT", "N"]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert not v.errors

    def test_bad_file_with_other_letters(self, tmp_path):
        contents = ["HGCT", "GADT", "TTTT", "BBBB"]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert len(v.errors) == 1
        assert "Errors on lines 1, 2, 4." in v.errors[0]

    def test_bad_file_with_numbers(self, tmp_path):
        contents = ["2GCT", "GA1T", "TTTT", "444"]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert len(v.errors) == 1
        assert "Errors on lines 1, 2, 4." in v.errors[0]

    def test_bad_file_with_special_characters(self, tmp_path):
        contents = ["AGCT", "GA#T", "8CT$", "TTTT", "></+"]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert len(v.errors) == 1
        assert "Errors on lines 2, 3, 5." in v.errors[0]

    def test_bad_file_with_whitespace(self, tmp_path):
        contents = ["AGCT", "TTTT", "GA T", "ACTG ", " "]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert len(v.errors) == 1
        assert "Errors on lines 3, 4, 5." in v.errors[0]

    def test_bad_file_multiple_issues(self, tmp_path):
        contents = ["AGCT", "T2TT", "GABT", " ACTG", " ", "4", "ATCG"]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert len(v.errors) == 1
        assert "Errors on lines 2, 3, 4, 5, 6." in v.errors[0]

    def test_bad_file_truncate_errors(self, tmp_path):
        contents = ["wrong"] * 21
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert len(v.errors) == 1
        assert "21 lines have errors." in v.errors[0]

    def test_find_file_good(self, tmp_path):
        contents = ["AGCT", "GACT", "TTTT"]
        self.create_file(tmp_path, contents)
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == True
        assert v.collect_errors() == [None]

    def test_find_file_good_shared_global(self, tmp_path):
        path = Path(tmp_path / "global")
        contents = ["AGCT", "GACT", "TTTT"]
        self.create_file(path, contents)
        v = BarcodeValidator([path, Path(tmp_path / "non_global")], "Paired-Tag")
        assert v.find_and_check_barcode_file(path) == True
        assert v.collect_errors() == [None]

    def test_find_file_good_shared_nonglobal(self, tmp_path):
        path = Path(tmp_path / "non_global")
        contents = ["AGCT", "GACT", "TTTT"]
        self.create_file(path, contents)
        v = BarcodeValidator([path, Path(tmp_path / "global")], "Paired-Tag")
        assert v.find_and_check_barcode_file(path) == True
        assert v.collect_errors() == [None]

    def test_find_file_missing(self, tmp_path):
        v = BarcodeValidator([tmp_path], "Paired-Tag")
        assert v.find_and_check_barcode_file(tmp_path) == False
        assert v.collect_errors() == []

    def test_wrong_type(self, tmp_path):
        v = BarcodeValidator([tmp_path], "WrongType")
        assert v.plugin_valid == False
        assert v.collect_errors() == []
