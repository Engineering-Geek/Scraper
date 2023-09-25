import asyncio
import logging
from collections import defaultdict
from typing import List

import pandas as pd

from src.articles.news_article import NewsArticle
from src.utils.S3 import S3Bucket


def _batchify(articles: List[NewsArticle], max_batch: int = None) -> List[List[NewsArticle]]:
    """
    Batchify a list of news articles into smaller batches based on a maximum batch size
    while ensuring that articles from different domains are grouped together. This makes asynchronous
    scraping less likely to overwhelm any site.

    Args:
        articles (List[NewsArticle]): A list of NewsArticle objects to be batched.
        max_batch (int): The maximum size of each batch.

    Returns:
        List[List[NewsArticle]]: A list of batches, where each batch is a list of NewsArticle objects.

    Note:
        - This function takes a list of NewsArticle objects and groups them into batches.
        - The maximum batch size is specified by the `max_batch` parameter.
        - Articles from the same domain are not grouped together within a batch.
        - If the total number of articles is not a multiple of `max_batch`, the last batch may contain fewer articles.
    """
    # Create a dictionary to group articles by their domain
    hashtable = defaultdict(list)
    for article in articles:
        domain = article.domain
        hashtable[domain].append(article)

    # Initialize batches
    batches = []
    current_batch = []
    batching_done = False
    dynamic = not bool(max_batch)

    while not batching_done:
        hashtable_temp = hashtable.copy()
        # If dynamic batching is enabled, determine the maximum batch size
        max_batch = len(hashtable.keys()) if dynamic else max_batch
        for domain in hashtable.keys():
            # Add articles to the current batch
            current_batch.append(hashtable[domain][0])

            # Remove the article from the dictionary
            hashtable_temp[domain].pop(0)
            # If the domain is empty, remove it from the dictionary
            if len(hashtable_temp[domain]) == 0:
                del hashtable_temp[domain]

            # Check if the batch is full
            if len(current_batch) >= max_batch:
                batches.append(current_batch)
                current_batch = []
                break
        hashtable = hashtable_temp
        if len(hashtable.keys()) == 0:
            batches.append(current_batch)
            batching_done = True

    return batches


async def _scrape_batch(batch: List[NewsArticle]) -> List[dict]:
    """
    Asynchronously scrape a batch of news articles.

    Args:
        batch (List[NewsArticle]): A list of NewsArticle objects to be scraped.

    Returns:
        List[dict]: A list of metadata dictionaries for the scraped articles.
    """
    metadata_list = []

    # Asynchronously download articles
    async def download_article(article: NewsArticle):
        if not article.downloaded:
            success = article.download()
            if not success:
                logging.info(f"Downloading {article.url} failed")

    await asyncio.gather(*[download_article(article) for article in batch])

    # Asynchronously parse articles
    async def parse_article(article: NewsArticle):
        if not article.parsed:
            success = article.parse()
            if not success:
                logging.info(f"Parsing {article.url} failed")

    await asyncio.gather(*[parse_article(article) for article in batch])

    # Asynchronously apply NLP to articles
    async def apply_nlp_article(article: NewsArticle):
        if not article.nlp_applied:
            success = article.nlp()
            if not success:
                logging.info(f"Applying NLP to {article.url} failed")

    await asyncio.gather(*[apply_nlp_article(article) for article in batch])

    for article in batch:
        metadata_list.append(article.metadata())

    return metadata_list


async def article_scraper(articles: List[NewsArticle], max_batch: int = None, delay: float = 0.0,
                          save_path: str = None, s3_path: str = None, s3_name: str = None) -> pd.DataFrame:
    """
    Batchify a list of news articles into smaller batches and scrape them asynchronously.

    Args:
        articles (List[NewsArticle]): A list of NewsArticle objects to be batched and scraped.
        max_batch (int): The maximum size of each batch.
        delay (float): The number of seconds to wait between batches.
        save_path (str): The path to save the pandas dataframe to.
        s3_path (str): The path to save the pandas dataframe to in the S3 bucket.
        s3_name (str): The name of the S3 bucket.

    Returns:
        pd.DataFrame: A dataframe containing metadata dictionaries for the scraped articles.
    """

    batches = _batchify(articles, max_batch)

    # Create a list to store the results of each batch
    results = []

    for batch in batches:
        results.extend(await asyncio.gather(*[_scrape_batch(batch)]))
        await asyncio.sleep(delay)
    results = [item for sublist in results for item in sublist]
    df = pd.DataFrame(results).dropna(axis="rows")
    logging.info(f"Total number of articles: {len(df)}")

    logging.debug(f"Scraping ForEx data")

    if save_path:
        df.to_csv(save_path)

    if s3_path and s3_name:
        bucket = S3Bucket(s3_name)
        bucket.upload_dataframe(df, s3_path)
    elif s3_name or s3_path:
        logging.error("Both s3_path and s3_name must be specified if you want to upload to an S3 bucket")

    return df
