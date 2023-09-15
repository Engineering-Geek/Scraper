import asyncio
import logging
from abc import ABC, abstractmethod
import random
from typing import List, Union, Tuple
from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup
import pandas as pd
from requests import Response
from datetime import date, timedelta
from utils.S3 import S3Bucket
import requests
from scrapingant_client import ScrapingAntClient
from fake_useragent import UserAgent
from tqdm import tqdm


class AsyncLinkScraper(ABC):
    def __init__(self, bucket_name: str, use_browser: bool = False, save_page_source: bool = False,
                 file_log_level=logging.INFO, console_log_level=logging.DEBUG, log_filepath: str = 'scraper.log',
                 log_name: str = None):
        """
        Initialize a LinkScraper instance.

        Args:
            bucket_name (str): The name of the S3 bucket for storing scraped data.
            use_browser (bool): Whether to use a browser for scraping.
            save_page_source (bool): Whether to save the page source when scraping.
            file_log_level (int): The log level for file logging.
            console_log_level (int): The log level for console logging.
            log_filepath (str): The path to the log file.
            log_name (str): The name that will appear when logging.
            num_pages (int): Number of Pages to scrape
        """
        self.bucket = S3Bucket(bucket_name)
        self.use_browser = use_browser
        self.save_page_source = save_page_source
        self.file_log_level = file_log_level
        self.console_log_level = console_log_level
        self.log_filepath = log_filepath
        self.num_pages = 1

        self.logger = self._logger_setup(log_name)
        self.ua = UserAgent()

    def _logger_setup(self, log_name):
        """
        Logger setup for the LinkScraper.

        Returns:
            logging.Logger: The configured logger.
        """
        logger = logging.getLogger(self.__class__.__name__ if not log_name else log_name)
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

    @abstractmethod
    def _url(self, query: str, start_date: date, end_date: date, page_num: int) -> str:
        """
        Generate the URL for scraping.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            page_num (int): The page number

        Returns:
            str: The URL for scraping.
        """
        raise NotImplementedError

    async def _fetch(self, session: ClientSession, url: str) -> Union[Response, None]:
        """
        Fetch data asynchronously.

        Args:
            session (ClientSession): An aiohttp ClientSession.
            url (str): The URL to fetch.

        Returns:
            Union[Response, None]: The response or None if an error occurs.
        """
        try:
            async with session.get(url, headers={'User-Agent': self.ua.random}) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            self._handle_error("Error while fetching data", e)
            return None
    
    async def _fetch_and_parse(self, session: ClientSession, url: str) -> List[str]:
        """
        Fetch and parse HTML content asynchronously.

        Args:
            session (ClientSession): An aiohttp ClientSession.
            url (str): The URL to fetch and parse.

        Returns:
            List[str]: A list of extracted links.
        """
        # Generate a random sleep duration between 1 and 3 seconds (adjust as needed)
        sleep_duration = random.uniform(5, 10)
        await asyncio.sleep(sleep_duration)

        content = await self._fetch(session, url)
        if content is not None:
            try:
                return self.parse_content(content)
            except Exception as e:
                self._handle_error(f"Error in parsing {url[:20]}", e)
        else:
            self._log(f"Received {None} when fetching/parsing a URL, can't determine much more", logging.INFO)
            return []


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

    async def raw_data(self, session: ClientSession, query: str, start_date: date, end_date: date) -> List[List[str]]:
        """
        Fetch raw data from a source asynchronously.

        Args:
            session (ClientSession): An aiohttp ClientSession.
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.

        Returns:
            List[List[str]]: A list of extracted links for each page.
        """
        tasks = []
        for page_num in range(1, self.num_pages + 1):
            url = self._url(query, start_date, end_date, page_num)
            tasks.append(self._fetch_and_parse(session, url))

        return await asyncio.gather(*tasks)

    async def scrape(self, query: str, date_range: List[Tuple[date, date]]) -> pd.DataFrame:
        """
        Scrape data asynchronously for a given query and date range.

        Args:
            query (str): The search query.
            date_range (List[Tuple[date, date]]): A list of date ranges to scrape.

        Returns:
            pd.DataFrame: A DataFrame containing the scraped data.
        """
        rows = []
        async with ClientSession(connector=TCPConnector(ssl=False)) as session:
            for start_date, end_date in tqdm(date_range, f"{query}, start: {date_range[0][0]} - end: {date_range[-1][1]}"):
                try:
                    result_contents = await self.raw_data(session, query, start_date, end_date)
                    links = [link for sublist in result_contents for link in sublist]
                    row = {"start date": start_date, "end date": end_date, "links": links}
                    rows.append(row)
                    self._log(f"Scraped data for query: {query}, start date: {start_date}, end date: {end_date}",
                              level=logging.DEBUG)
                except Exception as e:
                    self._handle_error("Error while parsing raw data, check inherited class", e)

        df = pd.DataFrame(rows)
        return df


    def run(self, query: str, start_date: date, end_date: date, method: str = 'daily') -> Union[pd.DataFrame, None]:
        """
        Perform the scraping operation.
        The filepath in the S3 bucket will always be "Links/{query}.csv"

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
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
                      f"method: {method}")
            df = self.scrape(query, date_range)
            self.bucket.upload_dataframe(df, f"Links/{query}.csv")
            self._log(f"Scraping completed for query: {query}, start date: {start_date}, end date: {end_date}, "
                      f"method: {method}")
            return df
        else:
            self._log(f"No date range to scrape for query: {query}, start date: {start_date}, end date: {end_date}, "
                      f"method: {method}")


class AsyncGoogleNewsLinkScraper(AsyncLinkScraper):
    def __init__(self, bucket_name: str, console_level=logging.ERROR, file_log_level=logging.INFO, log_filepath: str = "scraper.log"):
        """
        Initialize a GoogleNewsLinkScraper instance. Running the start method will also upload the dataframe to the
            AWS S3 bucket.

        Args:
            api_key (str): The API key for ScrapingAnt.
            bucket_name (str): The name of the S3 bucket for storing scraped data.
            console_level (int): The log level for console logging.
            file_log_level (int): The log level for file logging.
            log_filepath (str): The path to the log file.
            log_name (str): The name that will appear when logging.

        Returns:
            GoogleNewsLinkScraper: A GoogleNewsLinkScraper instance.


        Examples:
            >>> google_link_scraper = GoogleNewsLinkScraper('ScrapingAnt API Key', 'AWS S3 bucket name')
            >>> start_date, end_date = date(2020, 1, 1), date(2020, 1, 31)
            >>> proxy = False # Will not use ScrapingAnt as a proxy when scraping
            >>> queries = ['Apple, Google, NVIDIA, Real Estate']
            >>> dataframes = []
            >>> for query in queries:
            >>>     dataframes.append(google_link_scraper(
            >>>         query=query,
            >>>         start_date=start_date,
            >>>         end_date=end_date,
            >>>         s3_filename=f'NewsURL/GoogleNews/{query}.csv',
            >>>         proxy=proxy
            >>>     ))
        """
        super().__init__(bucket_name, log_name=self.__class__.__name__,
                         console_log_level=console_level, file_log_level=file_log_level, 
                         log_filepath=log_filepath)

    def _url(self, query: str, start_date: date, end_date: date, page_num: int) -> str:
        """
        Generate the Google News URL for a specific query and date range.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            page_num (int): The page number for the search

        Returns:
            str: The generated Google News URL.
        """
        # Convert the start and end dates to the required format (MM/DD/YYYY)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')

        # Calculate the number of results to skip based on the page number
        results_per_page = 10  # Assuming 10 results per page
        results_to_skip = (page_num - 1) * results_per_page

        # Construct the base URL
        base_url = 'https://www.google.com/search?q='

        # Construct the date range parameter
        date_param = f'&tbs=cdr:1,cd_min:{start_date_str},cd_max:{end_date_str}'

        # Construct the news search parameter
        news_param = '&tbm=nws'

        # Construct the "start" parameter to skip results on subsequent pages
        start_param = f'&start={results_to_skip}'

        # Combine all the components to form the full URL
        full_url = f'{base_url}{query}{date_param}{news_param}{start_param}'

        return full_url

    def parse_content(self, content: Response) -> List[str]:
        """
        Parse the HTML content of a Google News page and extract links.

        Args:
            content (Response): The response object containing the HTML content.

        Returns:
            List[str]: A list of extracted links.
        """
        try:
            # Check the response status code
            if content.status_code != 200:
                self._handle_error(f"Response status code is {content.status_code}")
                return []

            # Fetch and parse HTML content
            html_content = content.text
            soup = BeautifulSoup(html_content, 'html.parser')
            # Find all links with class 'WlydOe' (you can use a different selector if needed)
            link_classes = soup.find_all('a', class_='WlydOe')
            links = [link['href'] for link in link_classes]
            return links
        except Exception as e:
            self._handle_error("Error while parsing the content", e)
            return []

    def __call__(self, query: str, start_date: date, end_date: date, num_pages: int = 5):
        """
        Scrape Google News for links related to a specific query and date range.
        The filepath in the S3 bucket will always be "Links/{query}.csv"

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            s3_filename (str): The filename for storing the scraped data in S3.
            proxy (bool): Whether to use a proxy for scraping.

        Examples:
            >>> google_link_scraper = GoogleNewsLinkScraper('AWS S3 bucket name')
            >>> start_date, end_date = date(2020, 1, 1), date(2020, 1, 31)
            >>> queries = ['Apple, Google, NVIDIA, Real Estate']
            >>> dataframes = []
            >>> for query in queries:
            >>>     dataframes.append(google_link_scraper(
            >>>         query=query,
            >>>         start_date=start_date,
            >>>         end_date=end_date,
            >>>     ))
        """
        try:
            self.num_pages = num_pages
            return self.run(query, start_date, end_date, 'daily')
        except Exception as e:
            self._handle_error("Error while scraping Google News", e)
            return pd.DataFrame()
            
            
if __name__ == "__main__":
    # Example usage:
    loop = asyncio.get_event_loop()
    google_link_scraper = GoogleNewsLinkScraper('AWS S3 bucket name')
    start_date, end_date = date(2020, 1, 1), date(2020, 1, 31)
    queries = ['Apple, Google, NVIDIA, Real Estate']
    dataframes = []

    for query in queries:
        dataframes.append(loop.run_until_complete(google_link_scraper(query=query, start_date=start_date, end_date=end_date)))
