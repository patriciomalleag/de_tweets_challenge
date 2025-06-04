# tests/test_pandas_backend.py

from __future__ import annotations

import sys
import os

root_src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, root_src)

import json
import pytest
from datetime import date

import pandas as pd

from backend.pandas_backend import (
    top_active_dates,
    top_emojis,
    top_mentioned_users,
    _load_ndjson,
)

# -------------------------------------------------------------------------- #
# ───────────────────────────────  FIXTURES  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

@pytest.fixture
def sample_data_path(tmp_path):
    data = [
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": "Hello 👋 World 🌍",
            "mentionedUsers": [{"username": "user2"}, {"username": "user3"}]
        },
        {
            "date": "2024-03-20T11:00:00+0000",
            "user": {"username": "user2"},
            "content": "Test 🎉",
            "mentionedUsers": [{"username": "user1"}]
        },
        {
            "date": "2024-03-21T10:00:00+0000",
            "user": {"username": "user1"},
            "content": "Another day 🌞",
            "mentionedUsers": [{"username": "user2"}]
        }
    ]
    
    file_path = tmp_path / "test_data.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    
    return file_path

@pytest.fixture
def sample_data_with_nulls(tmp_path):
    data = [
        {
            "date": None,
            "user": None,
            "content": "Test without date and user",
            "mentionedUsers": None
        },
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": None,
            "mentionedUsers": []
        }
    ]
    
    file_path = tmp_path / "test_data_with_nulls.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    
    return file_path

# -------------------------------------------------------------------------- #
# ───────────────────────────────  TESTS  ────────────────────────────────── #
# -------------------------------------------------------------------------- #

class TestLoadNDJSON:
    
    def test_load_ndjson_basic(self, tmp_path):
        test_data = [{"col1": 1, "col2": 2}]
        file_path = tmp_path / "test.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for item in test_data:
                f.write(json.dumps(item) + "\n")

        df = _load_ndjson(file_path, ["col1"])
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["col1"]
        assert df["col1"].tolist() == [1]

    def test_load_ndjson_invalid_path(self):
        with pytest.raises(FileNotFoundError):
            _load_ndjson("invalid/path.ndjson", ["col1"])

    def test_load_ndjson_multiple_columns(self, tmp_path):
        test_data = [{"col1": 1, "col2": 2, "col3": 3}]
        file_path = tmp_path / "test.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for item in test_data:
                f.write(json.dumps(item) + "\n")
        
        df = _load_ndjson(file_path, ["col1", "col3"])
        
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == {"col1", "col3"}
        assert df["col1"].iloc[0] == 1
        assert df["col3"].iloc[0] == 3

class TestTopActiveDates:
    
    def test_top_active_dates_basic(self, sample_data_path):
        result = top_active_dates(sample_data_path, n=2)
        
        assert len(result) == 2
        assert isinstance(result[0][0], date)
        assert isinstance(result[0][1], str)
        assert result[0][0] == date(2024, 3, 20)
        assert result[1][0] == date(2024, 3, 21)

    def test_top_active_dates_empty_data(self, tmp_path):
        empty_file = tmp_path / "empty.ndjson"
        empty_file.touch()
        
        result = top_active_dates(empty_file)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_top_active_dates_with_nulls(self, sample_data_with_nulls):
        result = top_active_dates(sample_data_with_nulls)
        
        assert len(result) == 1
        assert result[0][0] == date(2024, 3, 20)
        assert isinstance(result[0][1], str)

    def test_top_active_dates_n_parameter(self, sample_data_path):
        result = top_active_dates(sample_data_path, n=1)
        
        assert len(result) == 1
        assert result[0][0] == date(2024, 3, 20)

class TestTopEmojis:
    
    def test_top_emojis_basic(self, sample_data_path):
        result = top_emojis(sample_data_path, n=3)
        assert len(result) > 0
        assert all(isinstance(item[0], str) for item in result)
        assert all(isinstance(item[1], int) for item in result)
        emojis_in_data = {"👋", "🌍", "🎉", "🌞"}
        for emoji, _ in result:
            assert emoji in emojis_in_data

    def test_top_emojis_no_emojis(self, tmp_path):
        data = [{"content": "No emojis here"}]
        file_path = tmp_path / "no_emojis.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
        
        result = top_emojis(file_path)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_top_emojis_with_nulls(self, sample_data_with_nulls):
        result = top_emojis(sample_data_with_nulls)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_top_emojis_n_parameter(self, sample_data_path):
        result = top_emojis(sample_data_path, n=1)
        
        assert len(result) == 1
        assert isinstance(result[0][0], str)
        assert isinstance(result[0][1], int)

class TestTopMentionedUsers:
    
    def test_top_mentioned_users_basic(self, sample_data_path):
        result = top_mentioned_users(sample_data_path, n=3)
        
        assert len(result) > 0
        assert all(isinstance(item[0], str) for item in result)
        assert all(isinstance(item[1], int) for item in result)
        possible_users = {"user1", "user2", "user3"}
        for username, _ in result:
            assert username in possible_users

    def test_top_mentioned_users_no_mentions(self, tmp_path):
        data = [{"mentionedUsers": []}]
        file_path = tmp_path / "no_mentions.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
        
        result = top_mentioned_users(file_path)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_top_mentioned_users_with_nulls(self, sample_data_with_nulls):
        result = top_mentioned_users(sample_data_with_nulls)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_top_mentioned_users_n_parameter(self, sample_data_path):
        result = top_mentioned_users(sample_data_path, n=1)
        
        assert len(result) == 1
        assert isinstance(result[0][0], str)
        assert isinstance(result[0][1], int)
        assert result[0][0] == "user2"
