from django.db import models
from djongo import models
# Create your models here.


class News(models.Model):
    title = models.TextField()
    abstract = models.TextField()
    author = models.CharField(max_length=200)
    source = models.CharField(max_length=200)
    published_date = models.DateTimeField()
    content = models.TextField()
    url = models.URLField()
