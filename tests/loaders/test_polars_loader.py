import polars as pl
import pytest
from src.backend.polars_backend import _lazy_scan


class TestPolarsLoader:

    def test_lazy_scan_ok(self, tmp_path):
        fp = tmp_path / "data.ndjson"
        fp.write_text('{"x":1,"y":2}\n')
        lf = _lazy_scan(fp, ["x"])
        assert isinstance(lf, pl.LazyFrame)
        assert lf.collect().columns == ["x"]


    def test_lazy_scan_bad_path(self):
        with pytest.raises(FileNotFoundError):
            _lazy_scan("missing.ndjson", ["x"])
