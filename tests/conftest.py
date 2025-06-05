# tests/conftest.py
from importlib import import_module
import pytest


_BACKENDS = [
    "src.backend.duckdb_backend",
    "src.backend.pandas_backend",
    "src.backend.polars_backend",
    "src.backend.ijson_backend",
]


@pytest.fixture(params=_BACKENDS, ids=[p.split(".")[1] for p in _BACKENDS])
def be(request):
    return import_module(request.param)


from tests.data_fixtures import (
    ndjson_path,
    sample_data_path,
    sample_data_with_nulls,
    sample_data_no_emoji,
)
