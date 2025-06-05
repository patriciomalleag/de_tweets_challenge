class TestTopMentionedUsers:

    def test_basic(self, be, sample_data_path):
        assert be.top_mentioned_users(sample_data_path, n=3)[0][0] == "user2"


    def test_with_nulls(self, be, sample_data_with_nulls):
        assert be.top_mentioned_users(sample_data_with_nulls) == []
