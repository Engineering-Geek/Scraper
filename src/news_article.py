import logging
from datetime import date
from typing import List, Union, Dict

from newspaper import Article
import tldextract

from src.query import Query


class NewsArticle:
    """
    Represents a news article.

    Args:
        query (Query): The query associated with the article.
        url (str): The URL of the article.

    Attributes:
        url (str): The URL of the article.
        query (Query): The query associated with the article.
        title (str): The title of the article.
        text (str): The full text content of the article.
        authors (List[str]): List of authors of the article.
        publish_date (date): The publication date of the article.
        summary (str): A summary of the article content.
        downloaded (bool): True if the article is downloaded, False otherwise.
        parsed (bool): True if the article is parsed, False otherwise.
        nlp_applied (bool): True if NLP processing is applied, False otherwise.

    Methods:
        download(): Downloads the article content.
        parse(): Parses the article content.
        nlp(): Applies NLP processing to the article content.
        metadata(): Returns metadata of the article if all processing steps are completed; otherwise, returns False.
    """

    def __init__(self, query: Query, url: str):
        self.url = url
        self.domain = tldextract.extract(url).domain
        self.query = query

        self.article = Article(url)

        self.title: str = ""
        self.text: str = ""
        self.authors: List[str] = []
        self.publish_date: date = date(1970, 1, 1)
        self.summary: str = ""

        self.downloaded = False
        self.parsed = False
        self.nlp_applied = False

    def download(self) -> bool:
        """
        Downloads the article content.

        Returns:
            bool: True if the download is successful, False otherwise.
        """
        try:
            logging.debug(f'Downloading {self.url}')
            self.article.download()
            logging.debug(f'Downloaded {self.url}')
            self.downloaded = True
            return True
        except Exception as e:
            logging.warning(f'Error downloading {self.url}: {str(e)}')
            return False

    def parse(self) -> bool:
        """
        Parses the article content.

        Returns:
            bool: True if parsing is successful, False otherwise.
        """
        try:
            logging.debug(f'Parsing {self.url}')
            self.article.parse()
            logging.debug(f'Parsed {self.url}')

            self.title = self.article.title
            self.text = self.article.text
            self.authors = self.article.authors
            self.publish_date = self.article.publish_date

            self.parsed = True
            return True
        except Exception as e:
            logging.warning(f'Error parsing {self.url}: {str(e)}')
            return False

    def nlp(self) -> bool:
        """
        Applies NLP processing to the article content.

        Returns:
            bool: True if NLP processing is successful, False otherwise.
        """
        try:
            logging.debug(f'Applying NLP to {self.url}')
            self.article.nlp()
            logging.debug(f'Applied NLP to {self.url}')

            self.summary = self.article.summary

            self.nlp_applied = True
            return True
        except Exception as e:
            logging.warning(f'Error Applying NLP to {self.url}: {str(e)}')
            return False

    def metadata(self) -> Union[Dict[str, Any]]:
        """
        Returns metadata of the article if all processing steps are completed.

        Returns:
            Union[Dict[str, Any], bool]: A dictionary of metadata if processing is complete, False otherwise.
        """
        if not self.publish_date:
            return dict()
        if self.downloaded and self.parsed:
            return {
                'url': self.url,
                'query': self.query.query,
                'title': self.title,
                'text': self.text,
                'authors': self.authors,
                'publish_date': self.publish_date,
                'summary': self.summary
            }
        else:
            return dict()

    def __str__(self):
        if self.title == "":
            return self.domain
        else:
            return self.title

    def __repr__(self):
        return self.__str__()
