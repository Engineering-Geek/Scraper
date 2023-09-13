from datetime import date
from typing import List
from bs4 import BeautifulSoup
import pandas as pd
from requests import Response
from utils.S3 import S3Bucket
from scrapers.links.link_scraper import LinkScraper
from utils.link_generators import google_news_url
import logging

class GoogleNewsLinkScraper(LinkScraper):
    def __init__(self, api_key: str, bucket_name: str):
        """
        Initialize a GoogleNewsLinkScraper instance.

        Args:
            api_key (str): The API key for ScrapingAnt.
            bucket_name (str): The name of the S3 bucket for storing scraped data.
        """
        super().__init__(api_key, bucket_name)

    def _url(self, query: str, start_date: date, end_date: date) -> str:
        """
        Generate the Google News URL for a specific query and date range.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.

        Returns:
            str: The generated Google News URL.
        """
        return google_news_url(query, start_date, end_date)

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

    def scrape_google_news(self, query: str, start_date: date, end_date: date, s3_filename: str, proxy: bool = False):
        """
        Scrape Google News for links related to a specific query and date range.

        Args:
            query (str): The search query.
            start_date (date): The start date for the search.
            end_date (date): The end date for the search.
            s3_filename (str): The filename for storing the scraped data in S3.
            proxy (bool): Whether to use a proxy for scraping.
        """
        try:
            return self(query, start_date, end_date, s3_filename, 'daily', proxy)
        except Exception as e:
            self._handle_error("Error while scraping Google News", e)
