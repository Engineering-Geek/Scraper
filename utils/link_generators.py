from datetime import date


def google_news_url(query: str, start_date: date, end_date: date) -> str:
    # Convert the start and end dates to the required format (MM/DD/YYYY)
    start_date_str = start_date.strftime('%m/%d/%Y')
    end_date_str = end_date.strftime('%m/%d/%Y')

    # Construct the base URL
    base_url = 'https://www.google.com/search?q='

    # Construct the date range parameter
    date_param = f'&tbs=cdr:1,cd_min:{start_date_str},cd_max:{end_date_str}'

    # Construct the news search parameter
    news_param = '&tbm=nws'

    # Combine all the components to form the full URL
    full_url = f'{base_url}{query}{date_param}{news_param}'

    return full_url


def marketwatch_url(query: str, start_date: date, end_date: date) -> str:
    # Convert the start and end dates to the required format (MM/DD/YYYY)
    start_date_str = start_date.strftime('%m/%d/%Y')
    end_date_str = end_date.strftime('%m/%d/%Y')

    # Construct the URL
    base_url = 'https://www.marketwatch.com/search?q='
    date_param = f'&sd={start_date_str}&ed={end_date_str}'
    query_param = f'&tab=All%20News'
    full_url = f'{base_url}{query}{date_param}{query_param}'

    return full_url
