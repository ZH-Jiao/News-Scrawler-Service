# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
import requests
from .models import News
from .scrawler import Scrawler, CNNScrawler
from .TimeMeasure import TimeMeasure

@TimeMeasure
@api_view(['GET'])
def get_nyt(request):

    print(request.query_params)
    news_count = 10 if 'count' not in request.query_params else int(request.query_params['count'])
    url = ('https://api.nytimes.com/svc/news/v3/content/all/all.json?' +
           'api-key=RMECIpcQ8lqEVb99JYSiASgkPX7Ras23')
    response = requests.get(url)
    data = response.json()['results']
    scraper = Scrawler()
    list_to_write = []
    for i in range(min(news_count, len(data))):
        d = data[i]
        entry = News(
            title=d['title'],
            abstract=d['abstract'],
            author=None,
            source=d['source'],
            published_date=d['created_date'],
            url=d['url'],
            content=scraper.read_url(d['url']).get_content_by_p(),
        )
        entry.save()
        # Compose Json and save to kafka
        news_dict = {
            'title': d['title'],
            'abstract': d['abstract'],
            'author': None,
            'source': d['source'],
            'published_date': d['created_date'],
            'url': d['url'],
            'content': scraper.read_url(d['url']).get_content_by_p()
        }
        list_to_write.append(news_dict)

    scraper.kafka.write_dict_batch(list_to_write)
    print(len(list_to_write))
    return Response()

@api_view(['GET'])
def get_cnn(request):
    url = 'https://www.cnn.com'
    url = 'https://www.cnn.com/us'
    cnn_scrawler = CNNScrawler()
    cnn_scrawler.get_cnn_news_from_home(url)
    return Response()


