import asyncio
import json
from typing import Dict, List, Coroutine, Union, Any

import pandas as pd
from requests import Response
from scrapingant_client import ScrapingAntClient
from utils.S3 import S3Bucket
from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup
import aiohttp
import re
import random
import logging
from abc import ABC, abstractmethod


class LinkScraper(ABC):
    def __init__(self, api_key: str, bucket_name: str, browser: bool = False, page_source: bool = False,
                 user_agents_path: str = 'user_agents.txt', max_concurrent_scrapes: int = 5):
        """
        Initialize a LinkScraper instance.

        Args:
            api_key (str): The API key for the scraping service.
            bucket_name (str): The name of the S3 bucket for storing scraped data.
            browser (bool, optional): Whether to use a browser for scraping. Defaults to False.
            page_source (bool, optional): Whether to retrieve the page source in addition to links. Defaults to False.
            user_agents_path (str, optional): The path to a file containing user-agent strings. Defaults to
                'user_agents.txt'.
            max_concurrent_scrapes (int, optional): The maximum number of concurrent scrapes. Defaults to 5.
        """
        self.api_key = api_key
        self.client = ScrapingAntClient(api_key)
        self.bucket = S3Bucket(bucket_name)

        self.browser = browser
        self.page_source = page_source
        self.user_agents = self._load_valid_user_agents(user_agents_path)

        self.logger = logging.getLogger(__name__)
        self.semaphore = asyncio.Semaphore(max_concurrent_scrapes)

    def _handle_error(self, message: str, e: Exception):
        """
        Handle and log errors.

        Args:
            message (str): A description of the error.
            e (Exception): The exception object.
        """
        self.logger.error(f"{message}: {str(e)}")

    def _load_valid_user_agents(self, user_agents_path: str):
        """
        Load and validate user-agent strings from a file.

        Args:
            user_agents_path (str): The path to a file containing user-agent strings.

        Returns:
            List[str]: A list of valid user-agent strings.
        """
        # Define a regular expression pattern to match a common user-agent format
        user_agent_pattern = (r'^Mozilla/\d+\.\d+ \((Windows|Macintosh|iPhone|iPad|Linux); .+ AppleWebKit/\d+\.\d+ \('
                              r'.+ Gecko/.+ Chrome/\d+\.\d+\.\d+\ Safari/\d+\.\d+\)$')
        try:
            valid_agents = []
            with open(user_agents_path, 'r') as f:
                for user_agent in f.readlines():
                    user_agent = user_agent.strip()
                    if re.match(user_agent_pattern, user_agent):
                        valid_agents.append(user_agent)
            return valid_agents

        except FileNotFoundError as e:
            # Handle the case where the file does not exist
            self._handle_error("User agent file not found", e)
            return []

        except Exception as e:
            # Handle other exceptions, e.g., file read or parsing errors
            self._handle_error("Error loading user agents", e)
            return []

    @abstractmethod
    def _url(self, query: str, start_date: date, end_date: date) -> str:
        """Generates the URL for the query given the start date and end date

        Args:
            query (str): The term to be searched
            start_date (date): date object for the start date
            end_date (date): date object for the end date

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            str: The final URL
        """
        raise NotImplementedError

    async def _fetch_without_proxy(self, url) -> Union[Response, None]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                    response.raise_for_status()  # Raise an exception if the HTTP request fails
                    return await response
        except Exception as e:
            self._handle_error("Error while fetching data without proxy", e)
            return None

    async def _fetch_with_proxy(self, url: str) -> Union[Response, None]:
        try:
            return await self.client.general_request_async(url, headers={'User-Agent': random.choice(self.user_agents)})
        except Exception as e:
            self._handle_error("Error while fetching data with proxy", e)
            return None

    async def get_links(self, query: str, start_date: date, end_date: date, proxy: bool) -> List[str]:
        """
        Parse data from a website.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            proxy (bool): Whether to use a proxy for the request.

        Returns:
            Any: The parsed data.
        """
        try:
            result_content = await self.raw_data(query, start_date, end_date, proxy)
        except Exception as e:
            self._handle_error("Error while getting raw data", e)
            result_content = None
        try:
            if result_content is not None:
                parsed_data = self.parse_content(result_content)
                return parsed_data
            else:
                return []
        except Exception as e:
            self._handle_error("Error while parsing raw data, check inherited class", e)
            return []

    @abstractmethod
    def parse_content(self, content: Response) -> List[str]:
        """
        Parse the links from the webpage

        Args:
            content (Response): The HTML or text content of the web page.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            List[str]: a list of URLs for the news
        """
        raise NotImplementedError

    async def raw_data(self, query: str, start_date: date, end_date: date, proxy: bool) -> Union[Response, None]:
        url = self._url(query, start_date, end_date)

        try:
            if proxy:
                result_content = await self._fetch_with_proxy(url)
            else:
                result_content = await self._fetch_without_proxy(url)
            return result_content
        except Exception as e:
            self._handle_error(f"Error when getting the raw data for query: {query}, start date: {start_date}, "
                               f"end date: {end_date}, proxy: {proxy}", e)
            return None

    async def scrape_with_semaphore(self, query, start_date, end_date, proxy):
        async with self.semaphore:
            return await self.get_links(query, start_date, end_date, proxy)

    async def scrape_date_range(self, query, date_range, proxy):
        tasks = [self.scrape_with_semaphore(query, start_date, end_date, proxy) for start_date, end_date in date_range]
        return await asyncio.gather(*tasks)

    def __call__(self, query: str, start_date: date, end_date: date, s3_filename: str, method: str = 'daily',
                 proxy: bool = False) -> Coroutine[Any, Any, bool]:
        """
        Start the scraping process with specified date range and method.

        Args:
            query (str): The query for the search
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            s3_filename (str): The remote file csv name to save the dataframe to
            method (str, optional): The scraping method, can be 'daily', 'monthly', or 'weekly'. Defaults to 'daily'.

        Raises:
            ValueError: If an unsupported scraping method is provided.

        Returns:
            bool: if the dataframe has been successfully saved to the S3 bucket

        Example:
            >>> my_scraper = LinkScraper('your_api_key', 'your_bucket_name')
            >>> query = "Apple"
            >>> start_date = date(2023, 1, 1)
            >>> end_date = date(2023, 1, 7)
            >>> success = my_scraper(query, start_date, end_date, method='weekly')
        """
        # Determine the number of days between start_date and end_date based on the chosen method
        try:
            if method == 'daily':
                date_range = [(start_date + timedelta(days=x), start_date + timedelta(days=x)) for x in
                              range((end_date - start_date).days)]
            elif method == 'weekly':
                # Implement logic for weekly scraping
                date_range = [(start_date + timedelta(weeks=i), start_date + timedelta(weeks=i + 1)) for i in
                              range((end_date - start_date).days // 7)]
            else:
                raise ValueError("Unsupported scraping method. Supported methods are 'daily' and 'weekly'.")
        except Exception as e:
            self._handle_error("User did not give a valid method. Accepts only 'daily' or 'weekly'.", e)
            date_range = []

        df = pd.DataFrame(columns=["start date", "end date", "links"])
        if len(date_range) != 0:
            links = asyncio.run(self.scrape_date_range(query, date_range, proxy))
            for start_date, end_date, link in zip(date_range, links):
                df.loc[len(df.index)] = [start_date, end_date, link]
        success = self.bucket.upload_dataframe(df, s3_filename)
        return success

