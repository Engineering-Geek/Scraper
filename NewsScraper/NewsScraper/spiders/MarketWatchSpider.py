import scrapy

class MarketWatchSpider(scrapy.Spider):
    name = 'apple_news'
    start_urls = ['https://www.marketwatch.com/investing/stock/aapl']

    def parse(self, response):
        # Extract Apple-related news headlines and links
        news_items = response.css('.element__title')
        for news_item in news_items:
            headline = news_item.css('::text').get()
            link = response.urljoin(news_item.css('a::attr(href)').get())
            
            yield {
                'headline': headline,
                'link': link
            }

        # Follow pagination links if available (assuming there are multiple pages)
        next_page = response.css('.pagination__next a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)
