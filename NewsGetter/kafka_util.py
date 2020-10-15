import json

from bson import json_util
from pykafka import KafkaClient


class MyKafka:

    def __init__(self):
        self.producer = None
        self.client = KafkaClient(hosts='127.0.0.1:9092')
        print(self.client.topics)
        self.topic = self.client.topics['NewsEntity']
        print(self.topic)

    def set_topic(self, topic):
        self.topic = self.client.topics[topic]
        if self.topic is None:
            raise RuntimeError('Topic ' + str(topic) + ' not found')

    def start_producer(self, topic):
        self.set_topic(topic)
        self.producer = self.topic.get_sync_producer()

    def write_string(self, data):
        if not isinstance(data, str):
            raise TypeError(str(data) + ' is not a string')

        with self.topic.get_sync_producer() as producer:
            producer.produce(bytes(data, encoding='utf-8'))

    def write_dict(self, data, topic=None):
        self.producer.produce(json.dumps(data, default=json_util.default).encode('utf-8'))

    def write_dict_batch(self, data, topic=None):
        self.set_topic(topic)
        with self.topic.get_sync_producer() as producer:
            for d in data:
                producer.produce(json.dumps(d, default=json_util.default).encode('utf-8'))


if __name__ == '__main__':
    kafka_client = MyKafka()
    message = {'title': 'Fake Title',
               'author': 'JZH',
               'a_list': [1, 2, 3, 4, 5]}
    kafka_client.write_dict(message)



