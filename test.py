import asyncio
from scrapers.links.async_link_scraper import AsyncGoogleNewsLinkScraper
from scrapers.links.sync_link_scraper import GoogleNewsLinkScraper
from utils.S3 import S3Bucket
from datetime import date
import asyncio



async def main_async():
    # Example usage:
    loop = asyncio.get_event_loop()
    google_link_scraper = AsyncGoogleNewsLinkScraper('test-debug-nm')
    start_date, end_date = date(2020, 1, 1), date(2020, 1, 31)
    queries = ['Apple', 'Google', 'NVIDIA', 'Real Estate']
    dataframes = []

    for query in queries:
        dataframes.append(await google_link_scraper.scrape(query=query, date_range=[(start_date, end_date)]))

    # You can work with dataframes here or return them as needed.
    for df in dataframes:
        print(df)
        
def main_sync():
    # Example usage:
    loop = asyncio.get_event_loop()
    google_link_scraper = GoogleNewsLinkScraper('test-debug-nm')
    queries = ['Apple', 'Google', 'NVIDIA', 'Real Estate']
    dataframes = []

    for query in queries:
        dataframes.append(google_link_scraper.scrape(query=query, start=date(2023, 1, 1), end=date(2023, 1, 31)))

    # You can work with dataframes here or return them as needed.
    for df in dataframes:
        print(df)

if __name__ == "__main__":
    # asyncio.run(main_async())
    main_sync()

