import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Tuple, List, Dict

import MetaTrader5
import asyncio

import pandas as pd
from tqdm import tqdm

from src.articles.query import Query
from src.articles.google import GoogleNewsLinkScraper
from src.ticker.metatrader_forex import initialize, exchange_rates, mt5
from src.articles.scraper import article_scraper
from src.utils.S3 import S3Bucket


class ForExScraper:
    """
    A class for scraping and analyzing foreign exchange (ForEx) data along with related news articles.

    Args:
        mt5_config_path (str): Path to the MetaTrader 5 (MT5) configuration file.
        currency_config_path (str): Path to the currency configuration file.
        scraper_config_path (str): Path to the scraper configuration file.

    Attributes:
        start (date): Start date for data retrieval.
        end (date): End date for data retrieval.
        mt5_config_path (str): Path to the MT5 configuration file.
        currency_config_path (str): Path to the currency configuration file.
        queries_dict (Dict[date, List[Query]]): Dictionary of queries for news articles.
        pages (int): Number of pages to scrape for each query.
        currency_pair (str): Currency pair being analyzed.
        scraper_config_path (str): Path to the scraper configuration file.
        bucket (S3Bucket): S3 bucket for storing scraped data.
        s3_root_dir (str): Root directory in the S3 bucket.
        delay (float): Delay between batches during scraping.
        max_batch (int): Maximum batch size for scraping.

    Methods:
        run(): Run the ForEx data and news scraping process.
        close(): Shutdown the MT5 connection.
    """

    def __init__(self, mt5_config_path: str, currency_config_path: str, scraper_config_path: str):
        """
        Initialize the ForExScraper.

        Args:
            mt5_config_path (str): Path to the MT5 configuration file.
            currency_config_path (str): Path to the currency configuration file.
            scraper_config_path (str): Path to the scraper configuration file.
        """
        self.start: date = None
        self.end: date = None
        self.mt5_config_path = mt5_config_path
        self._initialize()
        self.currency_config_path = currency_config_path
        self.queries_dict, self.pages, self.currency_pair = self._create_queries()
        self.scraper_config_path = scraper_config_path
        self.bucket, self.s3_root_dir, self.delay, self.max_batch = self._parse_scraper_settings()

    def _initialize(self):
        logging.debug(f"Creating {__class__.__name__} using filepath {self.mt5_config_path}")
        try:
            initialized = initialize(self.mt5_config_path)
        except Exception as e:
            logging.warning(f"Unable to initialize MT5\nMT5 Warning: {mt5.last_error()}\nCustom Warning: {e}")
            initialized = False
        initialized = mt5.last_error()[1] == 'Success' if not initialized else initialized
        if initialized:
            logging.debug("MetaTrader5 Login successful")
        else:
            logging.error(f"Initialization failed: {str(mt5.last_error())}")

    def _create_queries(self) -> Tuple[Dict[date, List[Query]], int, str]:
        logging.debug("Creating Queries")
        try:
            logging.debug(f"Attempting to open currency config path: {self.currency_config_path}")
            cfg = json.load(open(self.currency_config_path))
            logging.debug(f"Opening currency config path successful")
        except Exception as e:
            logging.error(f"Error opening currency config path at {self.currency_config_path}: {str(e)}")
            raise e  # No point in continuing once this error is reached; nothing else will work

        currency_pair = cfg["currency pair"]
        self.start = date(*cfg["start year, month, day"])
        self.end = date(*cfg["end year, month, day"]) if cfg["end year, month, day"] is not None else date.today()
        pages = cfg["number of pages"]

        days = [self.start + timedelta(days=n) for n in range((self.end - self.start).days)]
        queries = {}
        for day in days:
            queries.update({
                day: [Query(query, day, day) for query in cfg["search terms"]]
            })
        return queries, pages, currency_pair

    def _parse_scraper_settings(self):
        logging.debug("Loading Scraper Settings")
        try:
            logging.debug(f"Attempting to open scraper config path: {self.scraper_config_path}")
            cfg = json.load(open(self.scraper_config_path))
            logging.debug(f"Opening scraper config path successful")
        except Exception as e:
            logging.error(f"Error opening scraper config path at {self.scraper_config_path}: {str(e)}")
            raise e  # No point in continuing once this error is reached; nothing else will work

        test_bucket = cfg["test bucket"]
        production_bucket = cfg["production bucket"]
        s3_root_dir = cfg["s3 root dir"]
        delay = float(cfg["delay"])
        max_batch = int(cfg["max batch"])
        bucket = production_bucket if production_bucket else test_bucket
        bucket = S3Bucket(bucket)

        return bucket, s3_root_dir, delay, max_batch

    def close(self):
        """
        Shutdown the MetaTrader 5 (MT5) connection.
        """
        logging.info(f"Shutting down {self.__class__.__name__}: {self.currency_pair}")
        mt5.shutdown()

    def _upload(self, day: date, df: pd.DataFrame, filename: str) -> bool:
        directory = (os.path.join(self.s3_root_dir, self.currency_pair, str(day.year), str(day.month), str(day.day)).
                     replace("\\", "/"))
        path = os.path.join(directory, filename).replace("\\", "/")
        upload_success = self.bucket.upload_dataframe(df, path)
        logging.debug(f"Result of uploading DataFrame on S3 Bucket {self.bucket.bucket_name} {upload_success}: "
                      f"{'success' if upload_success else 'fail'}")
        return upload_success

    async def _news(self):
        """
        Run the News Scraper (asynchronously)
        """
        logging.info(f"Starting Google News and Article Scraper for {self.currency_pair}")
        google_news_scraper = GoogleNewsLinkScraper()
        days = tqdm([self.start + timedelta(days=day) for day in range((self.end - self.start).days)],
                    f"Gathering news from {self.start} - {self.end}")
        for day in days:
            news_articles = google_news_scraper(self.queries_dict[day], self.pages)
            news_df = await article_scraper(news_articles, self.max_batch, self.delay)
            self._upload(day, news_df, "news.csv")

    async def news(self):
        asyncio.run(self._news())

    def forex(self):
        """
        Run the Forex Scraper
        """
        logging.info(f"Starting ForEx Ticker Scraper for {self.currency_pair}")
        days = tqdm([self.start + timedelta(days=day) for day in range((self.end - self.start).days)],
                    f"Gathering news from {self.start} - {self.end}")
        for day in days:
            forex_df = exchange_rates(
                pair=self.currency_pair,
                timeframe=MetaTrader5.TIMEFRAME_M1,
                start=datetime(day.year, day.month, day.day, 0, 0, 0),
                end=datetime(day.year, day.month, day.day, 23, 59, 59)
            )
            self._upload(day, forex_df, "forex.csv")


def main():
    x = ForExScraper(
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\MT5.json',
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\EURUSD.json',
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\scraper_settings.json'
    )
    asyncio.run(x.run())


if __name__ == "__main__":
    main()
