# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
import requests
from .models import News
from .scrawler import Scrawler, CNNScrawler

@api_view(['GET'])
def get_nyt(request):

    url = ('https://api.nytimes.com/svc/news/v3/content/all/all.json?' +
           'api-key=RMECIpcQ8lqEVb99JYSiASgkPX7Ras23')
    response = requests.get(url)
    data = response.json()['results']
    scraper = Scrawler()
    for i in range(1):
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
    return Response()

@api_view(['GET'])
def get_cnn(request):
    url = 'https://www.cnn.com'
    url = 'https://www.cnn.com/us'
    cnn_scrawler = CNNScrawler()
    cnn_scrawler.get_cnn_news_from_home(url)
    return Response()


