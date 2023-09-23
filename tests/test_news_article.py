from datetime import date
from articles.query import Query
from articles.news_article import NewsArticle


def test_download():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    article = NewsArticle(query, "https://example.com/sample-article")

    assert article.download() is True  # Assuming the download is successful


def test_parse():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    article = NewsArticle(query, "https://example.com/sample-article")

    assert article.parse() is True  # Assuming parsing is successful


def test_nlp():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    article = NewsArticle(query, "https://example.com/sample-article")

    assert article.nlp() is True  # Assuming NLP processing is successful


def test_metadata():
    query = Query("Sample Query", date(2023, 1, 1), date(2023, 1, 5))
    article = NewsArticle(query, "https://example.com/sample-article")

    # Assuming all processing steps are completed successfully
    article.download()
    article.parse()
    article.nlp()

    metadata = article.metadata()

    assert isinstance(metadata, dict)
    assert 'url' in metadata
    assert 'query' in metadata
    assert 'title' in metadata
    assert 'text' in metadata
    assert 'authors' in metadata
    assert 'publish_date' in metadata
    assert 'summary' in metadata
