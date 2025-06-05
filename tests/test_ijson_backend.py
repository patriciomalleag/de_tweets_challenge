# tests/test_ijson_backend.py
from __future__ import annotations

import sys
import os
import json
from datetime import date
from pathlib import Path

import pytest

root_src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, root_src)

from backend.ijson_backend import (
    top_active_dates,
    top_emojis,
    top_mentioned_users,
    _check_file_exists,
)

# -------------------------------------------------------------------------- #
# ───────────────────────────────  FIXTURES  ─────────────────────────────── #
# -------------------------------------------------------------------------- #

@pytest.fixture
def sample_data_path(tmp_path):
    data = [
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

    file_path = tmp_path / "test_data.ndjson"
    with file_path.open("w", encoding="utf-8") as f:
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
            "mentionedUsers": None,
        },
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": None,
            "mentionedUsers": [],
        },
    ]

    file_path = tmp_path / "test_data_with_nulls.ndjson"
    with file_path.open("w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    return file_path


@pytest.fixture
def sample_data_with_malformed_records(tmp_path):
    data = [
        {
            "date": "2024-99-99T00:00:00+0000",
            "user": {"username": "evil"},
            "content": "Bad date 💥",
            "mentionedUsers": [{"username": "x"}],
        },
        {
            "date": "2024-03-22T10:00:00+0000",
            "user": "not_a_dict",
            "content": "Wrong user 🤖",
            "mentionedUsers": [{"username": "y"}],
        },
        {
            "date": "2024-03-22T11:00:00+0000",
            "user": {"username": None},
            "content": "Null username 🙈",
            "mentionedUsers": [{"username": "z"}],
        },
        {
            "date": "2024-03-23T08:00:00+0000",
            "user": {"username": "user_ok"},
            "content": None,
            "mentionedUsers": None,
        },
        {
            "date": "2024-03-23T09:00:00+0000",
            "user": {"username": "user_ok"},
            "content": "Mention anomaly 😺",
            "mentionedUsers": "not_a_list",
        },
        {
            "date": "2024-03-23T10:00:00+0000",
            "user": {"username": "valid_user"},
            "content": "All good 😺😺",
            "mentionedUsers": [{"username": "valid_user"}],
        },
    ]

    file_path = tmp_path / "malformed.ndjson"
    with file_path.open("w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    return file_path


@pytest.fixture
def sample_data_with_no_emoji(tmp_path):
    data = [
        {
            "date": "2024-03-20T10:00:00+0000",
            "user": {"username": "user1"},
            "content": "No emoji here",
            "mentionedUsers": [],
        },
    ]

    file_path = tmp_path / "no_emoji.ndjson"
    with file_path.open("w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    return file_path


# -------------------------------------------------------------------------- #
# ─────────────────────────────  TEST HELPERS  ───────────────────────────── #
# -------------------------------------------------------------------------- #

class TestCheckFileExists:
    def test_check_file_exists_valid(self, tmp_path):
        valid_file = tmp_path / "exists.ndjson"
        valid_file.write_text("")
        _check_file_exists(valid_file)

    def test_check_file_exists_invalid(self):
        with pytest.raises(FileNotFoundError):
            _check_file_exists(Path("nonexistent.ndjson"))


# -------------------------------------------------------------------------- #
# ───────────────────────────────  TESTS  ────────────────────────────────── #
# -------------------------------------------------------------------------- #

class TestTopActiveDates:
    def test_basic(self, sample_data_path):
        result = top_active_dates(sample_data_path, n=2)
        assert result == [
            (date(2024, 3, 20), "user1"),
            (date(2024, 3, 21), "user1"),
        ]

    def test_empty_file(self, tmp_path):
        empty = tmp_path / "empty.ndjson"
        empty.touch()
        assert top_active_dates(empty) == []

    def test_with_nulls(self, sample_data_with_nulls):
        res = top_active_dates(sample_data_with_nulls)
        assert res == [(date(2024, 3, 20), "user1")]

    def test_n_parameter(self, sample_data_path):
        assert len(top_active_dates(sample_data_path, n=1)) == 1

    def test_malformed_records(self, sample_data_with_malformed_records):
        res = top_active_dates(sample_data_with_malformed_records, n=5)
        assert res == [(date(2024, 3, 23), "user_ok")]


class TestTopEmojis:
    def test_basic(self, sample_data_path):
        result = top_emojis(sample_data_path, n=3)
        emojis_in_data = {"👋", "🌍", "🎉", "🌞"}
        assert {e for e, _ in result}.issubset(emojis_in_data)

    def test_no_emojis(self, sample_data_with_no_emoji):
        assert top_emojis(sample_data_with_no_emoji) == []

    def test_with_nulls(self, sample_data_with_nulls):
        assert top_emojis(sample_data_with_nulls) == []

    def test_n_parameter(self, sample_data_path):
        assert len(top_emojis(sample_data_path, n=1)) == 1


class TestTopMentionedUsers:
    def test_basic(self, sample_data_path):
        assert top_mentioned_users(sample_data_path, n=3)[0][0] == "user2"

    def test_malformed_records(self, sample_data_with_malformed_records):
        assert top_mentioned_users(sample_data_with_malformed_records) == [("valid_user", 1)]


class TestErrorHandling:
    def test_functions_raise_on_missing_file(self, tmp_path):
        bogus = tmp_path / "missing.ndjson"
        for fn in (top_active_dates, top_emojis, top_mentioned_users):
            with pytest.raises(FileNotFoundError):
                fn(bogus)
