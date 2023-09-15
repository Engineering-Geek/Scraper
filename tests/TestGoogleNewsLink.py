import logging
import pandas as pd
import pytest
from datetime import date
from scrapers.links.google_news_scraper import GoogleNewsLinkScraper
import json

# Create a logger for your tests
logger = logging.getLogger(__name__)


@pytest.fixture
def scraper():
    api_key = json.load(open('api_key.json'))['Scraping Ant']
    bucket_name = 'test-debug-nm'
    return GoogleNewsLinkScraper(api_key, bucket_name)


def test_generate_google_news_url(scraper: GoogleNewsLinkScraper):
    try:
        query = 'Apple'
        start_date = date(2020, 1, 1)
        end_date = date(2020, 1, 5)
        url = scraper._url(query, start_date, end_date, page_num=5)
        assert isinstance(url, str) and url.startswith('https://www.google.com/')
    except Exception as e:
        logger.error(f"Test failed with an error: {str(e)}")
        assert False


def test_scrape_google_news_success(scraper: GoogleNewsLinkScraper):
    try:
        query = 'Apple'
        start_date = date(2020, 1, 1)
        end_date = date(2020, 1, 5)
        s3_filename = 'test/apple.csv'
        df = scraper(query, start_date, end_date, s3_filename)
        assert isinstance(df, pd.DataFrame) and len(df) > 0
        assert isinstance(df['links'][0], list), "Dataframe Links are not a list"
        assert isinstance(df['links'][0][0], str), "Dataframe Links are not comprised of strings"
    except Exception as e:
        logger.error(f"Test failed with an error: {str(e)}")
        assert False


def test_scrape_google_news_error_handling(scraper: GoogleNewsLinkScraper):
    try:
        # Simulate an error by providing an invalid API key
        invalid_api_key = "invalid_key"
        scraper_with_error = GoogleNewsLinkScraper(invalid_api_key, 'test-bucket')
        query = 'Apple'
        start_date = date(2020, 1, 1)
        end_date = date(2020, 1, 5)
        s3_filename = 'test/apple.csv'
        scraper_with_error(query, start_date, end_date, s3_filename)
    except Exception as e:
        assert isinstance(e, Exception)
    else:
        logger.error("Test should have raised an exception, but it didn't.")
        assert False
