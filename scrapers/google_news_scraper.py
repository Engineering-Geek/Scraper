from datetime import date
from typing import List
from bs4 import BeautifulSoup
from requests import Response
from scrapers.links.async_link_scraper import LinkScraper
import logging


class GoogleNewsLinkScraper(LinkScraper):
    def __init__(self, api_key: str, bucket_name: str, console_level=logging.ERROR, file_log_level=logging.INFO, log_filepath: str = "scraper.log"):
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
        super().__init__(api_key, bucket_name, log_name=self.__class__.__name__,
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

    def __call__(self, query: str, start_date: date, end_date: date, proxy: bool = False,
                 num_pages: int = 5):
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
            >>>         proxy=proxy
            >>>     ))
        """
        try:
            self.num_pages = num_pages
            return self.run(query, start_date, end_date, 'daily', proxy)
        except Exception as e:
            self._handle_error("Error while scraping Google News", e)
