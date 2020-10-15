import json
import re
from urllib.error import URLError

import requests
from bs4 import BeautifulSoup
from requests import HTTPError

from NewsGetter.TimeMeasure import TimeMeasure
from NewsGetter.kafka_util import MyKafka


class NewsSource:
    """
    Interface for querying news from source and output json
    """

    def __init__(self):
        self.source_name = ''
        self.kafka_topic = ''

    def set_source(self, url):
        self.url = url

    def read_url_to_soup(self, url):
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        print(f'Url in soup: ' + url)
        req = requests.get(url, headers)
        req.encoding = 'utf-8'
        soup = BeautifulSoup(req.content, 'html.parser')
        return soup

    def get_news_dict(self, title=None, abstract=None, author=None, source=None,
                      published_date=None, url=None, content=None):
        news_dict = {
            'title': title,
            'abstract': abstract,
            'author': author,
            'source': source,
            'published_date': published_date,
            'url': url,
            'content': content
        }
        return news_dict

    def produce_news_to_kafka(self, count):
        pass

    def get_content_by_p(self, url):
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        req = requests.get(url, headers)
        req.encoding = 'utf-8'
        soup = BeautifulSoup(req.content, 'html.parser')

        if soup == None:
            return None
        content = []
        for p in soup.find_all('p'):
            content.append(p.text.strip())
        return ''.join(content)



class NYTSource(NewsSource):

    def __init__(self):
        super().__init__()
        self.soup = None
        self.url = ('https://api.nytimes.com/svc/news/v3/content/all/all.json?' +
                    'api-key=RMECIpcQ8lqEVb99JYSiASgkPX7Ras23')
        self.source_name = 'NYT'
        self.kafka_topic = 'News_feed_nyt'
        self.kafka = MyKafka()


    def produce_news_to_kafka(self, count=10):
        news_count = count
        url = self.url
        response = requests.get(url)
        data = response.json()['results']
        list_to_write = []
        for i in range(min(news_count, len(data))):
            d = data[i]
            # Compose Json and save to kafka

            list_to_write.append(self.get_news_dict(
                title=d['title'],
                abstract=d['abstract'],
                author=None,
                source=d['source'],
                published_date=d['created_date'],
                url=d['url'],
                content=self.get_content_by_p(d['url'])
            ))

        self.kafka.write_dict_batch(list_to_write, self.kafka_topic)
        return len(list_to_write)



class CNNSource(NewsSource):

    """
    take in a url of the cnn home page -> continuously write News to DB
    """
    pattern = re.compile("https://www.cnn.com/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/.*index.html")
    pattern_domain = re.compile("https://www.cnn.com/.*")

    def __init__(self):
        super().__init__()
        self.soup = None
        self.url = ('https://www.cnn.com/politics')
        self.source_name = 'CNN'
        self.kafka_topic = 'News_feed_cnn'
        self.kafka = MyKafka()
        self.visited = set()
        self.count = 0
        self.visited.add('https://www.cnn.com')
        self.kafka.start_producer(self.kafka_topic)

    def produce_news_to_kafka(self, count=10):
        print(f'Start at {self.url}')

        self.dfs(self.url, count)

    def dfs(self, url, max_count=100):
        if self.count >= max_count:
            return
        if url in self.visited:
            return
        if not re.match(self.pattern_domain, url):
            return
        print(f'Currently at {url}')
        self.visited.add(url)
        self.count += 1
        try:
            # print("The url fu: " + url)
            if re.match(self.pattern, url):
                # read single news
                self.get_cnn_news_page(url)


            # Generate sub nodes: get all url in this page and run recursion
            soup = self.read_url_to_soup(url)
            # Find links to articles
            # attrs = {'data-vr-contentbox': re.compile('.*')}
            # body = soup.find('body')
            print(soup)
            # re.compile('article.*')
            links = soup.find_all('article')
            print(f'len of Articles: {len(links)}')
            for link in links:
                # print(f'Generating nodes: {link.prettify()}')
                # print(f'In loop link: {link}')
                if 'data-vr-contentbox' not in link.attrs:
                    continue
                # print(link['data-vr-contentbox'])
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
        soup = self.read_url_to_soup(url)

        title = soup.find('meta', attrs={'itemprop': 'headline'}).attrs['content']
        authors = soup.find('meta', attrs={'itemprop': 'author'}).attrs['content']
        published_time = soup.find('meta', attrs={'itemprop': 'datePublished'}).attrs['content']
        abstract = soup.find('meta', attrs={'itemprop': 'description'}).attrs['content']
        contents = soup.find_all('section', _class=re.compile("zn zn-body-text.*"))
        contents_list = [content.text.strip() for content in contents]
        content = ''.join(contents_list)

        # Compose Json and save to kafka
        print(f'Writing ' + title)
        self.kafka.write_dict(self.get_news_dict(
            title=title,
            abstract=abstract,
            author=''.join(authors),
            source=self.source_name,
            published_date=published_time,
            url=url,
            content=soup.find('section', id="body-text").text
        ))



