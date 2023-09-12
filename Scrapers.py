import asyncio
import json
from typing import Dict, List
from scrapingant_client import ScrapingAntClient
from utils.S3 import S3Bucket
from datetime import date
import requests
from bs4 import BeautifulSoup
import aiohttp
import re
import random
import logging

class LinkScraper:
    def __init__(self, api_key: str, bucket_name: str, browser: bool = False, page_source: bool = False, user_agents_path: str = 'user_agents.txt'):
        """
        Initialize a LinkScraper instance.

        Args:
            api_key (str): The API key for the scraping service.
            bucket_name (str): The name of the S3 bucket for storing scraped data.
            browser (bool, optional): Whether to use a browser for scraping. Defaults to False.
            page_source (bool, optional): Whether to retrieve the page source in addition to links. Defaults to False.
            user_agents_path (str, optional): The path to a file containing user-agent strings. Defaults to 'user_agents.txt'.
        """
        self.api_key = api_key
        self.client = ScrapingAntClient(api_key)
        self.bucket = S3Bucket(bucket_name)
        
        self.browser = browser
        self.page_source = page_source
        self.user_agents = self.load_valid_user_agents(user_agents_path)

        # Configure logging
        logging.basicConfig(filename='scraping.log', level=logging.INFO)
    
    def load_valid_user_agents(self, user_agents_path):
        """
        Load and validate user-agent strings from a file.

        Args:
            user_agents_path (str): The path to a file containing user-agent strings.

        Returns:
            List[str]: A list of valid user-agent strings.
        """
        # Define a regular expression pattern to match a common user-agent format
        user_agent_pattern = r'^Mozilla/\d+\.\d+ \((Windows|Macintosh|iPhone|iPad|Linux); .+ AppleWebKit/\d+\.\d+ \(.+ Gecko/.+ Chrome/\d+\.\d+\.\d+\ Safari/\d+\.\d+\)$'

        valid_agents = []
        with open(user_agents_path, 'r') as f:
            for user_agent in f.readlines():
                user_agent = user_agent.strip()
                if re.match(user_agent_pattern, user_agent):
                    valid_agents.append(user_agent)
        return valid_agents
    
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
    
    async def _fetch_without_proxy(self, url):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={'User-Agent': random.choice(self.user_agents)}) as response:
                    response.raise_for_status()  # Raise an exception if the HTTP request fails
                    return await response.text()
        except Exception as e:
            logging.error(f"Error while fetching data without proxy: {str(e)}")
            return None

    async def _fetch_with_proxy(self, url):
        try:
            return await self.client.general_request_async(url, headers={'User-Agent': random.choice(self.user_agents)})
        except Exception as e:
            logging.error(f"Error while fetching data with proxy: {str(e)}")
            return None

    async def parse_data(self, query: str, start_date: date, end_date: date, proxy: bool):
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
        result_content = await self.raw_data(query, start_date, end_date, proxy)
        if result_content is not None:
            parsed_data = self.parse_content(result_content)
            return parsed_data
        else:
            return None

    def parse_content(self, content):
        """
        Parse content extracted from a web page.

        Args:
            content (str): The HTML or text content of the web page.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Returns:
            Any: The parsed data.
        """
        raise NotImplementedError
    
    async def raw_data(self, query: str, start_date: date, end_date: date, proxy: bool):
        url = self._url(query, start_date, end_date)
        
        if proxy:
            result_content = await self._fetch_with_proxy(url)
        else:
            result_content = await self._fetch_without_proxy(url)

        return result_content

            


class GeneralScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.client = ScrapingAntClient(api_key)
        self.bucket = S3Bucket()
    
    def google_news_url(self, query: str, start_date: date, end_date: date) -> str:
        # Convert the start and end dates to the required format (MM/DD/YYYY)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')

        # Construct the base URL
        base_url = 'https://www.google.com/search?q='

        # Construct the date range parameter
        date_param = f'&tbs=cdr:1,cd_min:{start_date_str},cd_max:{end_date_str}'

        # Construct the news search parameter
        news_param = '&tbm=nws'

        # Combine all the components to form the full URL
        full_url = f'{base_url}{query}{date_param}{news_param}'

        return full_url
    
    def marketwatch_url(self, query: str, start_date: date, end_date: date) -> str:
        # Convert the start and end dates to the required format (MM/DD/YYYY)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')

        # Construct the URL
        base_url = 'https://www.marketwatch.com/search?q='
        date_param = f'&sd={start_date_str}&ed={end_date_str}'
        query_param = f'&tab=All%20News'
        full_url = f'{base_url}{query}{date_param}{query_param}'

        return full_url
        

    async def scrape(self, url, name):
        await self.client.start_scraping(url, name)
        await self.client.wait_for_completion()
        await self.bucket.upload_file(name, f'{name}.json')


async def main():
    api_key = json.load(open('API_KEYS.json'))['Scraping Ant']
    client = ScrapingAntClient(api_key)


asyncio.run(main())

