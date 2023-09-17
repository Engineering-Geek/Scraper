from datetime import date, timedelta
from src import Query  # Replace 'your_module' with the actual module where Query is defined


def test_str():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    expected_str = "Query: Sample Query Date Range: 2023-01-01 - 2023-01-05"
    assert str(query) == expected_str


def test_repr():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    expected_repr = "Query: Sample Query Date Range: 2023-01-01 - 2023-01-05"
    assert repr(query) == expected_repr


def test_len():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    expected_length = 4  # The date range has 5 days, but we exclude the end date
    assert len(query) == expected_length


def test_call():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    initial_url_count = len(query.urls)
    query("http://example.com/news1")
    query("http://example.com/news2")
    new_url_count = len(query.urls)
    assert new_url_count == initial_url_count + 2
