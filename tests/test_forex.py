import os

import pandas as pd
import pytest
from datetime import date

from src.forex import ForExScraper
from utils.S3 import S3Bucket


@pytest.fixture
def forex_scraper() -> ForExScraper:
    mt5_config_path = r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\MT5.json'
    currency_config_path = r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\EURUSD.json'
    scraper_config_path = r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\scraper_settings.json'
    return ForExScraper(mt5_config_path, currency_config_path, scraper_config_path)


def test_initialization(forex_scraper: ForExScraper):
    assert isinstance(forex_scraper, ForExScraper)
    assert isinstance(forex_scraper.start, date)
    assert isinstance(forex_scraper.end, date)
    assert isinstance(forex_scraper.mt5_config_path, str)
    assert isinstance(forex_scraper.currency_config_path, str)
    assert isinstance(forex_scraper.queries_dict, dict)
    assert isinstance(forex_scraper.pages, int)
    assert isinstance(forex_scraper.currency_pair, str)
    assert isinstance(forex_scraper.scraper_config_path, str)
    assert isinstance(forex_scraper.bucket, S3Bucket)
    assert isinstance(forex_scraper.s3_root_dir, str)
    assert isinstance(forex_scraper.delay, float)
    assert isinstance(forex_scraper.max_batch, int)


def test_initialization_failure():
    mt5_config_path = 'invalid_path'
    currency_config_path = 'invalid_path'
    scraper_config_path = 'invalid_path'
    with pytest.raises(Exception):
        ForExScraper(mt5_config_path, currency_config_path, scraper_config_path)


@pytest.mark.asyncio
async def test_upload(forex_scraper: ForExScraper):
    await forex_scraper.run()
    bucket = forex_scraper.bucket

    day = forex_scraper.start
    directory = (os.path.join(forex_scraper.s3_root_dir, forex_scraper.currency_pair,
                              str(day.year), str(day.month), str(day.day)).replace("\\", "/"))
    news_path = os.path.join(directory, "news.csv").replace("\\", "/")
    forex_path = os.path.join(directory, "forex.csv").replace("\\", "/")

    news_df = bucket.get_dataframe(news_path)
    forex_df = bucket.get_dataframe(forex_path)

    assert isinstance(news_df, pd.DataFrame)
    assert isinstance(forex_df, pd.DataFrame)
    assert not news_df.empty
    assert not forex_df.empty


# Add more test cases as needed

if __name__ == "__main__":
    pytest.main()
