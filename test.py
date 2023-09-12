import asyncio
import json
from scrapingant_client import ScrapingAntClient


async def main():
    api_key = json.load(open('API_KEYS.json'))['Scraping Ant']
    client = ScrapingAntClient(api_key)
    client.


asyncio.run(main())

