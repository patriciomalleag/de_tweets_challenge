# tests/business/test_top_active_dates.py
from datetime import date


class TestTopActiveDates:

    def test_basic(self, be, sample_data_path):
        assert be.top_active_dates(sample_data_path, n=2) == [
            (date(2024, 3, 20), "user1"),
            (date(2024, 3, 21), "user1"),
        ]

    def test_with_nulls(self, be, sample_data_with_nulls):
        assert be.top_active_dates(sample_data_with_nulls) == [
            (date(2024, 3, 20), "user1")
        ]

    def test_n_parameter(self, be, sample_data_path):
        assert len(be.top_active_dates(sample_data_path, n=1)) == 1
