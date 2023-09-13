import logging
from abc import ABC, abstractmethod
from typing import List, Union
import pandas as pd
from requests import Response
from datetime import date, timedelta
from utils.S3 import S3Bucket
import random
import requests
from scrapingant_client import ScrapingAntClient

class LinkScraper(ABC):
    def __init__(self, api_key: str, bucket_name: str, use_browser: bool = False, save_page_source: bool = False,
                 user_agents_path: str = 'user_agents.txt',
                 file_log_level=logging.INFO, console_log_level=logging.DEBUG, log_filepath: str = 'scraper.log'):
        """
        Initialize a LinkScraper instance.

        Args:
            api_key (str): The API key for ScrapingAnt.
            bucket_name (str): The name of the S3 bucket for storing scraped data.
            use_browser (bool): Whether to use a browser for scraping.
            save_page_source (bool): Whether to save the page source when scraping.
            user_agents_path (str): The path to a file containing user agents for requests.
            file_log_level (int): The log level for file logging.
            console_log_level (int): The log level for console logging.
            log_filepath (str): The path to the log file.
        """
        self.proxy_client = ScrapingAntClient(api_key)
        self.bucket = S3Bucket(bucket_name)
        self.use_browser = use_browser
        self.save_page_source = save_page_source
        self.user_agents_path = user_agents_path
        self.file_log_level = file_log_level
        self.console_log_level = console_log_level
        self.log_filepath = log_filepath

        self.logger = self._setup_logger()
        self.user_agents = self._load_valid_user_agents(user_agents_path)

    def _setup_logger(self):
        """
        Setup the logger for the LinkScraper.

        Returns:
            logging.Logger: The configured logger.
        """
        logger = logging.getLogger(__class__.__name__)
        logger.setLevel(self.file_log_level)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(self.log_filepath)
        file_handler.setLevel(self.file_log_level)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.console_log_level)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def _log(self, message, level=logging.INFO):
        """
        Log a message.

        Args:
            message (str): The message to log.
            level (int): The log level.
        """
        if level == logging.DEBUG:
            self.logger.debug(message)
        elif level == logging.INFO:
            self.logger.info(message)
        elif level == logging.WARNING:
            self.logger.warning(message)
        elif level == logging.ERROR:
            self.logger.error(message)

    def _handle_error(self, message: str, e: Exception):
        """
        Handle and log an error.

        Args:
            message (str): The error message.
            e (Exception): The exception.
        """
        self._log(f"ERROR: {message}: {str(e)}", level=logging.ERROR)

    def _load_valid_user_agents(self, user_agents_path: str):
        """
        Load valid user agents from a file.

        Args:
            user_agents_path (str): The path to the user agents file.

        Returns:
            List[str]: A list of valid user agents.
        """
        try:
            valid_agents = []
            with open(user_agents_path, 'r') as f:
                for user_agent in f.readlines():
                    user_agent = user_agent.strip()
                    valid_agents.append(user_agent)
            return valid_agents
        except FileNotFoundError as e:
            self._handle_error("User agent file not found", e)
            return []
        except Exception as e:
            self._handle_error("Error loading user agents", e)
            return []

    @abstractmethod
    def _url(self, query: str, start_date: date, end_date: date) -> str:
        """
        Generate the URL for scraping.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.

        Returns:
            str: The URL for scraping.
        """
        raise NotImplementedError

    def _fetch_without_proxy(self, url) -> Union[Response, None]:
        """
        Fetch data without using a proxy.

        Args:
            url (str): The URL to fetch.

        Returns:
            Union[Response, None]: The response or None if an error occurs.
        """
        try:
            response = requests.get(url, headers={'User-Agent': random.choice(self.user_agents)})
            response.raise_for_status()
            return response
        except Exception as e:
            self._handle_error("Error while fetching data without proxy", e)
            return None

    def _fetch_with_proxy(self, url: str) -> Union[Response, None]:
        """
        Fetch data using a proxy.

        Args:
            url (str): The URL to fetch.

        Returns:
            Union[Response, None]: The response or None if an error occurs.
        """
        try:
            return self.proxy_client.general_request(url)
        except Exception as e:
            self._handle_error("Error while fetching data with proxy", e)
            return None

    @abstractmethod
    def parse_content(self, content: Response) -> List[str]:
        """
        Parse the content of a response and extract links.

        Args:
            content (Response): The response object.

        Returns:
            List[str]: A list of extracted links.
        """
        raise NotImplementedError

    def raw_data(self, query: str, start_date: date, end_date: date, use_proxy: bool) -> Union[Response, None]:
        """
        Fetch raw data from a source.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            use_proxy (bool): Whether to use a proxy for fetching.

        Returns:
            Union[Response, None]: The response or None if an error occurs.
        """
        url = self._url(query, start_date, end_date)

        try:
            if use_proxy:
                return self._fetch_with_proxy(url)
            else:
                return self._fetch_without_proxy(url)
        except Exception as e:
            self._handle_error(f"Error when getting the raw data for query: {query}, start date: {start_date}, "
                               f"end date: {end_date}, proxy: {use_proxy}", e)

    def scrape(self, query, date_range, use_proxy) -> pd.DataFrame:
        """
        Scrape data for a given query and date range.

        Args:
            query (str): The search query.
            date_range (List[Tuple[date, date]]): A list of date ranges to scrape.
            use_proxy (bool): Whether to use a proxy for scraping.

        Returns:
            pd.DataFrame: A DataFrame containing the scraped data.
        """
        rows = []
        for start_date, end_date in date_range:
            try:
                result_content = self.raw_data(query, start_date, end_date, use_proxy)
                if result_content is not None:
                    links = self.parse_content(result_content)
                    row = {"start date": start_date, "end date": end_date, "link": links}
                    rows.append(row)
                self._log(f"Scraped data for query: {query}, start date: {start_date}, end date: {end_date}", level=logging.DEBUG)
            except Exception as e:
                self._handle_error("Error while parsing raw data, check inherited class", e)
        
        df = pd.DataFrame(rows)
        return df

    def __call__(self, query: str, start_date: date, end_date: date, s3_filename: str, method: str = 'daily',
                 use_proxy: bool = False) -> Union[pd.DataFrame, None]:
        """
        Perform the scraping operation.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            s3_filename (str): The filename for storing the scraped data in S3.
            method (str): The scraping method ('daily' or 'weekly').
            use_proxy (bool): Whether to use a proxy for scraping.
            
        Returns:
            pd.DataFrame
        """
        try:
            if method == 'daily':
                date_range = [(start_date + timedelta(days=x), start_date + timedelta(days=x)) for x in
                              range((end_date - start_date).days)]
            elif method == 'weekly':
                date_range = [(start_date + timedelta(weeks=i), start_date + timedelta(weeks=i + 1)) for i in
                              range((end_date - start_date).days // 7)]
            else:
                raise ValueError("Unsupported scraping method. Supported methods are 'daily' and 'weekly'.")
        except Exception as e:
            self._handle_error("User did not give a valid method. Accepts only 'daily' or 'weekly'.", e)
            date_range = []

        if len(date_range) != 0:
            self._log(f"Scraping started for query: {query}, start date: {start_date}, end date: {end_date}, "
                      f"method: {method}, use_proxy: {use_proxy}")
            df = self.scrape(query, date_range, use_proxy)
            self.bucket.upload_dataframe(df, s3_filename)
            self._log(f"Scraping completed for query: {query}, start date: {start_date}, end date: {end_date}, "
                      f"method: {method}, use_proxy: {use_proxy}")
            return df
        else:
            self._log(f"No date range to scrape for query: {query}, start date: {start_date}, end date: {end_date}, "
                      f"method: {method}, use_proxy: {use_proxy}")

