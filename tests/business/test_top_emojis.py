class TestTopEmojis:

    def test_basic(self, be, sample_data_path):
        result = be.top_emojis(sample_data_path, n=3)
        assert {e for e, _ in result}.issubset({"👋", "🌍", "🎉", "🌞"})


    def test_no_emoji(self, be, sample_data_no_emoji):
        assert be.top_emojis(sample_data_no_emoji) == []


    def test_with_nulls(self, be, sample_data_with_nulls):
        assert be.top_emojis(sample_data_with_nulls) == []


    def test_n_parameter(self, be, sample_data_path):
        assert len(be.top_emojis(sample_data_path, n=1)) == 1
