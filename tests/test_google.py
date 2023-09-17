import logging
from datetime import date
from typing import List
from src.query import Query
from src.news_article import NewsArticle
from src.google import GoogleNewsLinkScraper


def test_google_news_link_scraper():
    scraper = GoogleNewsLinkScraper()
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    queries = [query]
    pages = 2  # Adjust the number of pages for testing

    articles = scraper(queries, pages)

    assert isinstance(articles, list)
    assert all(isinstance(article, NewsArticle) for article in articles)

    for article in articles:
        assert article.query == query
        assert article.publish_date in query.dates
        assert article.url.startswith("https://www.google.com/url?q=")
        assert article.downloaded is False
        assert article.parsed is False
        assert article.nlp_applied is False
