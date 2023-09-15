import asyncio
import newspaper
import aiohttp
import pandas as pd
from utils.S3 import S3Bucket


async def fetch_and_parse_article(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html_content = await response.text()

    article = newspaper.Article(url)
    article.download(input_html=html_content)
    article.parse()
    return article.title, article.text


def retrieve_frames(bucket_name: str):
    bucket = S3Bucket(bucket_name)
    dirpath = "Links/"
    dataframes = [bucket.get_dataframe(dirpath + file) for file in bucket.list_csv_files(dirpath)]
    print(dataframes[0].head())


async def main():
    urls = ["https://example.com/article1", "https://example.com/article2"]
    tasks = [fetch_and_parse_article(url) for url in urls]
    results = await asyncio.gather(*tasks)
    print(results)

# asyncio.run(main())
retrieve_frames('market-news-nm')
