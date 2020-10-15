from urllib.error import URLError, HTTPError

from bs4 import BeautifulSoup
import requests
import re
from .models import News
from .kafka_util import MyKafka


class Scrawler:
    """
    Parse url -> html to soup -> soup to content
    """

    def __init__(self):
        self.soup = None
        self.url = None
        self.kafka = MyKafka()

    def read_url_to_soup(self, url):
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        self.url = url
        req = requests.get(url, headers)
        req.encoding = 'utf-8'
        soup = BeautifulSoup(req.content, 'html.parser')
        return soup

    def get_content_by_p(self, soup=None):
        if soup is None:
            soup = self.soup

        if soup == None:
            return None
        content = []
        for p in soup.find_all('p'):
            content.append(p.text.strip())
        return ''.join(content)


class CNNScrawler(Scrawler):
    """
    take in a url of the cnn home page -> continuously write News to DB
    """
    pattern = re.compile("https://www.cnn.com/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/.*index.html")

    def __init__(self):
        super().__init__()
        self.visited = set()
        self.count = 0

    def get_cnn_news_from_home(self, url):
        print(f'Start at {url}')
        self.dfs(url)

    def dfs(self, url, max_count=20):
        if self.count >= max_count:
            return
        if url in self.visited:
            return
        print(f'Currently at {url}')
        self.visited.add(url)
        self.count += 1
        try:

            if re.match(self.pattern, url):
                # read single news
                self.get_cnn_news_page(url)

            # Generate sub nodes: get all url in this page and run recursion
            soup = self.read_url(url).soup
            # Find links to articles
            # attrs = {'data-vr-contentbox': re.compile('.*')}
            body = soup.find('body')
            links = body.find_all(re.compile('ar.*'))
            print(f'len of Articles: {len(links)}')
            for link in links:
                # print(f'Generating nodes: {link.prettify()}')
                # print(f'In loop link: {link}')
                if 'data-vr-contentbox' not in link.attrs:
                    continue
                print(link['data-vr-contentbox'])
                # link['data-vr-contentbox'] is /2020/09/25/politics/voting-rights-act-history-election-2020/index.html
                composed_link = "https://www.cnn.com" + link['data-vr-contentbox']
                if composed_link not in self.visited:
                    self.dfs(composed_link)

        except URLError as e:
            print(e)
            return
        except HTTPError as e:
            print(e)
            return

    def get_cnn_news_page(self, url):
        """
        Read the current soup, compose a single article in this page, and write to DB in News
        Example page: https://www.cnn.com/2020/09/25/politics/voting-rights-act-history-election-2020/index.html
        """
        print(f'## Getting news from {url}')
        soup = self.read_url(url).soup

        title = soup.find('meta', attrs={'itemprop': 'headline'}).attrs['content']
        authors = soup.find('meta', attrs={'itemprop': 'author'}).attrs['content']
        published_time = soup.find('meta', attrs={'itemprop': 'datePublished'}).attrs['content']
        abstract = soup.find('meta', attrs={'itemprop': 'description'}).attrs['content']
        contents = soup.find_all('section', _class=re.compile("zn zn-body-text.*"))
        contents_list = [content.text.strip() for content in contents]
        content = ''.join(contents_list)
        entry = News(
            title=title,
            abstract=abstract,
            author=''.join(authors),
            source="CNN",
            published_date=published_time,
            url=url,
            content=soup.find('section', id="body-text").text,
        )
        entry.save()

        # Compose Json and save to kafka
        news_dict = {
            'title': title,
            'abstract': abstract,
            'author': ''.join(authors),
            'source': 'CNN',
            'published_date': published_time,
            'url': url,
            'content': soup.find('section', id="body-text").text
        }
        self.kafka.write_dict(news_dict)

