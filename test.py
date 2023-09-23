import asyncio
from src.forex import ForExScraper
import logging


# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)


def main():
    x = ForExScraper(
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\MT5.json',
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\EURUSD.json',
        r'C:\Users\melgi\PycharmProjects\TraderAI\Scraper\cfg\scraper_settings.json'
    )
    asyncio.run(x.run())


if __name__ == "__main__":
    main()
