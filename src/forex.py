import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Tuple, List, Dict, Union

import MetaTrader5
import numpy as np
import asyncio

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from src.articles.query import Query
from src.articles.google import GoogleNewsLinkScraper
from src.ticker.metatrader_forex import initialize, exchange_rates, mt5
from src.articles.scraper import article_scraper
from src.articles.S3 import S3Bucket


class ForExScraper:
    def __init__(self, mt5_config_path: str, currency_config_path: str, scraper_config_path: str):
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
        initialized = initialize(self.mt5_config_path)
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

    def _get_forex_prices(self, day: date) -> pd.DataFrame:
        day = datetime(day.year, day.month, day.day)
        logging.info(f"Capturing prices for {self.currency_pair} for {day}")
        rates = exchange_rates(self.currency_pair, day, day)
        return rates

    def close(self):
        logging.info(f"Shutting down {self.__class__.__name__}: {self.currency_pair}")
        mt5.shutdown()

    def _upload(self, day: date, news_df: pd.DataFrame, forex_df: pd.DataFrame) -> Tuple[bool, bool]:
        directory = os.path.join(self.s3_root_dir, str(day.year), str(day.month), str(day.day))
        news_path = os.path.join(directory, "news.csv")
        forex_path = os.path.join(directory, "forex.csv")
        news_success = self.bucket.upload_dataframe(news_df, news_path)
        forex_success = self.bucket.upload_dataframe(forex_df, forex_path)
        logging.debug(f"Result of uploading News on S3 Bucket {self.bucket.bucket_name} {news_success}: "
                      f"{'success' if news_success else 'fail'}")
        logging.debug(f"Result of uploading Forex on S3 Bucket {self.bucket.bucket_name} {forex_success}: "
                      f"{'success' if forex_success else 'fail'}")
        return news_success, forex_success

    async def run(self) -> None:
        google_news_scraper = GoogleNewsLinkScraper()
        logging.info(f"Starting Google News Scraper for {self.currency_pair}")
        logging.info("Done getting news dataframe, getting forex exchange prices")

        forex_dataframes = {}
        days = tqdm([self.start + timedelta(days=day) for day in range((self.end - self.start).days)],
                    f"Gathering news from {self.start} - {self.end}")
        for day in days:
            days.set_postfix({"date": str(day)})
            news_articles = google_news_scraper(self.queries_dict[day], self.pages)
            news_df = await article_scraper(news_articles, self.max_batch, self.delay)
            forex_df = exchange_rates(
                pair=self.currency_pair,
                timeframe=MetaTrader5.TIMEFRAME_M1,
                start=datetime(day.year, day.month, day.day, 0, 0, 0),
                end=datetime(day.year, day.month, day.day, 23, 59, 59)
            )
            self._upload(day, news_df, forex_df)

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


def main():
    x = ForExScraper(
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\MT5.json',
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\EURUSD.json',
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\scraper_settings.json'
    )
    asyncio.run(x.run())


if __name__ == "__main__":
    main()
