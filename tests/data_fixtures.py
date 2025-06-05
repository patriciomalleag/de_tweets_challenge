# tests/data_fixtures.py
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Any

import pytest


# -------------------------------------------------------------------------- #
# ───────────────────────────────  HELPERS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def _write_ndjson(rows: List[Dict[str, Any]], path: Path) -> Path:
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row) + "\n")
    return path


@pytest.fixture
def ndjson_path(tmp_path):
    def _factory(rows: List[Dict[str, Any]], filename: str = "data.ndjson") -> Path:
        return _write_ndjson(rows, tmp_path / filename)

    return _factory


# -------------------------------------------------------------------------- #
# ───────────────────────────────  DATA FIXTURES  ────────────────────────── #
# -------------------------------------------------------------------------- #

@pytest.fixture
def sample_data_path(ndjson_path):
    rows = [
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": "Hello 👋 World 🌍",
            "mentionedUsers": [{"username": "user2"}, {"username": "user3"}],
        },
        {
            "date": "2024-03-20T11:00:00+0000",
            "user": {"username": "user2"},
            "content": "Test 🎉",
            "mentionedUsers": [{"username": "user1"}],
        },
        {
            "date": "2024-03-21T10:00:00+0000",
            "user": {"username": "user1"},
            "content": "Another day 🌞",
            "mentionedUsers": [{"username": "user2"}],
        },
    ]
    return ndjson_path(rows, "sample.ndjson")


@pytest.fixture
def sample_data_with_nulls(ndjson_path):
    rows = [
        {
            "date": None,
            "user": None,
            "content": "Test without date and user",
            "mentionedUsers": None,
        },
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": None,
            "mentionedUsers": [],
        },
    ]
    return ndjson_path(rows, "nulls.ndjson")


@pytest.fixture
def sample_data_no_emoji(ndjson_path):
    rows = [
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": "No emoji here",
            "mentionedUsers": [],
        }
    ]
    return ndjson_path(rows, "no_emoji.ndjson")
