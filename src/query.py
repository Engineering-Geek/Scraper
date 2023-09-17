from datetime import date, timedelta
from typing import List


class Query(object):
    """
    Represents a query for news articles within a specified date range.

    Args:
        query (str): The search query.
        start (date): The start date of the search range.
        end (date): The end date of the search range.
        log_file (str, optional): The log file path. Default is 'scraper.log'.

    Attributes:
        query (str): The search query.
        start (date): The start date of the search range.
        end (date): The end date of the search range.

    Methods:
        __str__(): Returns a string representation of the query.
        __repr__(): Returns a string representation of the query.
        __len__(): Returns the number of days in the date range.
        __getitem__(item): Allows indexing to retrieve the query and date within the range.
    """

    def __init__(self, query: str, start: date, end: date, log_file='scraper.log'):
        self.query = query
        self.start = start
        self.end = end
        self.dates = [start + timedelta(n) for n in range((end - start).days + 1)]
        self.urls: List[str] = []

    def __str__(self):
        return f'Query: {self.query} Date Range: {self.start} - {self.end}'

    def __repr__(self):
        return f'Query: {self.query} Date Range: {self.start} - {self.end}'

    def __len__(self) -> int:
        return (self.end - self.start).days

    def __call__(self, url: str):
        self.urls.append(url)