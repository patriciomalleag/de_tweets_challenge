# tests/loaders/test_duckdb_loader.py
from pathlib import Path
import duckdb
import pytest
from src.backend.duckdb_backend import _get_connection, _check_file_exists


class TestDuckDBLoaderHelpers:
    
    def test_get_connection_returns_memory_conn(self):
        con = _get_connection()
        assert isinstance(con, duckdb.DuckDBPyConnection)
        assert con.execute("SELECT 1").fetchone()[0] == 1
        con.close()


    def test_check_file_exists(self, tmp_path):
        fp = tmp_path / "ok.ndjson"
        fp.touch()
        _check_file_exists(fp)
        with pytest.raises(FileNotFoundError):
            _check_file_exists(Path("missing.ndjson"))
