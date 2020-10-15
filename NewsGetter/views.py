# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
import requests
from .models import News
from .scrawler import Scrawler, CNNScrawler
from .TimeMeasure import TimeMeasure
from .news_sourcing import NYTSource
from .news_sourcing import CNNSource

@api_view(['GET'])
def get_nyt(request):

    print(request.query_params)
    news_count = 10 if 'count' not in request.query_params else int(request.query_params['count'])
    source = NYTSource()
    result_count = source.produce_news_to_kafka(news_count)

    return Response('Successfully produced ' + str(result_count) + ' news')


@api_view(['GET'])
def get_cnn(request):
    news_count = 10 if 'count' not in request.query_params else int(request.query_params['count'])
    source = CNNSource()
    source.produce_news_to_kafka(news_count)
    return Response()


