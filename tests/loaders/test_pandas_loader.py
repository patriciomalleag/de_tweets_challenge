import pandas as pd
import pytest
from src.backend.pandas_backend import _load_ndjson


class TestPandasLoader:

    def test_load_ndjson_columns(self, tmp_path):
        fp = tmp_path / "file.ndjson"
        fp.write_text('{"a":1,"b":2}\n')
        df = _load_ndjson(fp, ["a"])
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["a"]


    def test_invalid_path(self):
        with pytest.raises(FileNotFoundError):
            _load_ndjson("nope.ndjson", ["x"])
