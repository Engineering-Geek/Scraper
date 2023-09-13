import pytest
from datetime import date
from scrapers.links.link_scraper import LinkScraper


# Mocking the scraping function
class MockLinkScraper(LinkScraper):
    async def get_links(self, query, start_date, end_date, proxy):
        # Simulate a list of scraped links without actual scraping
        return ['http://example.com/link1', 'http://example.com/link2']


@pytest.fixture
def link_scraper():
    # Create an instance of your LinkScraper with the mock class
    return MockLinkScraper(api_key='your_api_key', bucket_name='your_bucket_name')


def test_link_scraper(link_scraper):
    query = "Test Query"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 2)
    s3_filename = 'test_links.csv'
    method = 'daily'
    proxy = False

    # Call your LinkScraper function to test
    success = link_scraper(query, start_date, end_date, s3_filename, method, proxy)

    # Check if the scraping process was successful
    assert success


if __name__ == "__main__":
    pytest.main()
