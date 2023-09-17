import logging
from datetime import date
from typing import List
from fake_useragent import FakeUserAgent
from tqdm import tqdm

from bs4 import BeautifulSoup
import requests

from src.query import Query
from src.news_article import NewsArticle


class GoogleNewsLinkScraper:
    def __init__(self):
        self.user_agent = FakeUserAgent()
        logging.info(f'GoogleNewsLinkScraper initialized')

    @staticmethod
    def _google_news_url(query: str, query_date: date, page_num: int) -> str:
        """
        Generates a Google News search URL.

        Args:
            query (str): The search query.
            query_date (date): The date of the search.
            page_num (int): The page number for pagination.

        Returns:
            str: The constructed Google News search URL.
        """
        query = query.replace(' ', '+')
        query_date = query_date.strftime('%m/%d/%Y')
        results_to_skip = (page_num - 1) * 10

        # Construct the base URL
        base_url = 'https://www.google.com/search?q='

        # Construct the parameters
        date_param = f'&tbs=cdr:1,cd_min:{query_date},cd_max:{query_date}'
        news_param = '&tbm=nws'
        start_param = f'&start={results_to_skip}'

        url = f'{base_url}{query}{date_param}{news_param}{start_param}'
        return url

    def _get_news_articles(self, queries: List[Query], pages: int) -> List[NewsArticle]:
        articles = []
        for query in tqdm(queries, "Queries to iterate through"):
            q = query.query
            for q_date in tqdm(query.dates, f"Query: {q} --> {query.start} - {query.end}"):
                for page in range(1, pages + 1):
                    news_urls = self._get_links(self._google_news_url(q, q_date, page))
                    for news_url in news_urls:
                        articles.append(NewsArticle(query, news_url))
                        logging.info(f'Scraped Google News for query {q} on page {page}: {news_url}')
                    for article in articles:
                        article.publish_date = q_date
                    if len(articles) == 0:
                        break
        return articles

    def __call__(self, queries: List[Query], pages: int) -> List[NewsArticle]:
        """
        Scrapes Google News for articles matching specified queries and date ranges.

        This method retrieves news articles based on the provided queries and the specified number of pages to scrape.

        Args:
            queries (List[Query]): A list of Query objects representing search queries and date ranges.
            pages (int): The number of pages to scrape for each query.

        Returns:
            List[NewsArticle]: A list of NewsArticle objects representing the scraped news articles.

        Note:
            - This method scrapes Google News for the specified queries and pages, and it creates NewsArticle objects
                  for each article found.
            - Ensure that you have valid Query objects in the `queries` list, and that the S3Bucket object is properly
                  configured for storing article content.
        """
        return self._get_news_articles(queries, pages)

    def _get_links(self, google_url: str) -> List[str]:
        """
        Parse the HTML content of a Google News page and extract links.

        Args:
            google_url (str): The Google url to get news links from

        Returns:
            List[str]: A list of extracted links.
        """

        try:
            response = requests.get(google_url, headers={'User-Agent': self.user_agent.random})
            response.raise_for_status()
        except Exception as e:
            logging.warning(f"Error while fetching data: {e}")
            return []
        try:
            # Check the response status code
            if response.status_code != 200:
                logging.warning(f"Response status code is {response.status_code}")
                return []

            # Fetch and parse HTML content
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            # Find all links with class 'WlydOe' (you can use a different selector if needed)
            link_classes = soup.find_all('a', class_='WlydOe')
            links = [link['href'] for link in link_classes]
            return links
        except Exception as e:
            logging.warning(f"Error while parsing the content: {e}")
            return []
