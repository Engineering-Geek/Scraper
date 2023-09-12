import requests
from bs4 import BeautifulSoup
import random
from typing import List, Optional
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
import urllib
import socket
socket.setdefaulttimeout(200)

import os
print(os.getcwd())

class ProxyMiddleware(HttpProxyMiddleware):
    """
    Scrapy middleware for proxy rotation.

    This middleware fetches a list of proxies from a source (e.g., freeproxy.world) and rotates them for each request.

    Args:
        proxy_list_url (str): The URL of the source providing the proxy list.
        proxy_list_css_selector (str): The CSS selector to locate proxy elements in the HTML.
    
    Attributes:
        proxies (List[str]): A list of proxy URLs in the format 'http://ip:port'.
    """
    def __init__(self, encoding: str):
        self.proxy_list_url = 'https://www.freeproxy.world/?type=&anonymity=&country=&speed=100&port=&page=1'
        self.proxy_list_css_selector = 'show-ip-div'
        self.proxies: List[str] = []

    def fetch_proxies(self) -> List[str]:
        """
        Fetch and parse a list of proxy IP addresses and ports from the specified URL.

        Returns:
            List[str]: A list of proxy URLs in the format 'http://ip:port'.
        """
        try:
            response = requests.get(self.proxy_list_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            # Extract proxy IP addresses and ports based on the CSS selector
            proxies = []
            for element in soup.find_all('tr')[2:]:
                try:
                    ip = element.find('td', class_='show-ip-div').get_text().strip()
                    port = element.findAll('td')[1].get_text().strip()
                    proxy_url = f'http://{ip}:{port}'
                    proxies.append(proxy_url)
                except:
                    pass
            
            good_proxies = []
            for proxy in proxies:
                try:
                    print("TEST")
                    proxy_handler = urllib.request.ProxyHandler({'http': proxy})
                    print("a")
                    opener = urllib.request.build_opener(proxy_handler)
                    print("b")
                    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
                    print("c")
                    urllib.request.install_opener(opener)
                    print("d")
                    req=urllib.request.Request('http://www.example.com')
                    print("e")
                    sock=urllib.request.urlopen(req)
                    print("f")
                    good_proxies.append(proxy)
                except urllib.error.HTTPError as e:
                    print('Error code: ', e.code)
                except Exception as detail:
                    print("ERROR:", detail)
            
            with open('good_proxies.txt', 'w') as f:
                for proxy in good_proxies:
                    f.write(proxy + '\n')


            return good_proxies
        except requests.RequestException as e:
            raise ValueError(f"Error fetching proxies: {e}")

    def process_request(self, request, spider):
        """
        Process each request by rotating the proxy used for the request.

        Args:
            request: The Scrapy request being processed.
            spider: The Scrapy spider instance.
        """
        if not self.proxies:
            # Fetch proxies if the list is empty or periodically based on your requirements
            self.proxies = self.fetch_proxies()

        if self.proxies:
            request.meta['proxy'] = random.choice(self.proxies)



