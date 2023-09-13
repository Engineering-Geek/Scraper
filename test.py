import asyncio
from scrapers.links.google_news_scraper import GoogleNewsLinkScraper
from utils.S3 import S3Bucket
from datetime import date

def main():
    api_key = "28ba5ff845924218ba6b98280c9e8971"
    gns = GoogleNewsLinkScraper(api_key, 'market-news-nm')
    start_date = date(2020, 1, 1)
    end_date = date(2020, 1, 5)
    gns.scrape_google_news("Apple", start_date, end_date, 'links/apple.csv', proxy = False)
    see_df()

def see_df():
    bucket = S3Bucket('market-news-nm')
    df = bucket.get_dataframe('links/apple.csv')
    print(df)

if __name__ == "__main__":
    main()
