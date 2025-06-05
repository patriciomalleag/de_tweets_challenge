from pathlib import Path
import pytest
from src.backend.ijson_backend import _check_file_exists, _parse_ndjson_stream


class TestIjsonLoaderHelpers:

    def test_check_file_exists(self, tmp_path):
        fp = tmp_path / "exists.ndjson"
        fp.write_text("")
        _check_file_exists(fp)
        with pytest.raises(FileNotFoundError):
            _check_file_exists(Path("no-file.ndjson"))


    def test_parse_stream_yields_objects(self, tmp_path):
        fp = tmp_path / "data.ndjson"
        fp.write_text('{"a":1}\n{"b":2}\n')
        items = list(_parse_ndjson_stream(fp))
        assert items == [{"a": 1}, {"b": 2}]
