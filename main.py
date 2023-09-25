from src.forex import ForExScraper


MT5_CONFIG_PATH = 'C:\\Users\\melgi\\PycharmProjects\\TraderAI\\Scraper\\cfg\\MT5.json'
CURRENCY_CONFIG_PATH = 'C:\\Users\\melgi\\PycharmProjects\\TraderAI\\Scraper\\cfg\\EURUSD.json'
SCRAPER_CONFIG_PATH = 'C:\\Users\\melgi\\PycharmProjects\\TraderAI\\Scraper\\cfg\\scraper_settings.json'


def news():
    scraper = ForExScraper(MT5_CONFIG_PATH, CURRENCY_CONFIG_PATH, SCRAPER_CONFIG_PATH)
    scraper.news()


def forex():
    scraper = ForExScraper(MT5_CONFIG_PATH, CURRENCY_CONFIG_PATH, SCRAPER_CONFIG_PATH)
    scraper.forex()


if __name__ == "__main__":
    forex()
