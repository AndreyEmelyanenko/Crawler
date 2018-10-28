from elasticsearch import Elasticsearch
from elasticsearch import exceptions
from elasticsearch.connection import create_ssl_context
import datetime
import json


class ElasticsearchCrawlerClient(object):
    """ElasticsearchCrawlerClient is a CrawlerClient for Elasticsearch."""

    def __init__(self, url):
        """ElasticsearchCrawlerClient Constructor"""
        print(url)
        self.es = Elasticsearch(url)

    def contains(self, key):
        """External method 'contains' - checks, if this key already exists in Elasticsearch. Uses '__contains'."""
        self.__contains("fulltext", "all", key)

    def __contains(self, indexES, typeES, idES):
        """Internal method '__contains' - checks, if this key already exists in Elasticsearch."""
        try:
            res = self.es.get(index=indexES, doc_type=typeES, id=idES)
            print(res)
            print(res['_source'])
            return True
        except exceptions.NotFoundError as error:
            print(error)
            if error.status_code == 404:
                return False
            else:
                raise error

    def put(self, key, data, date, head, tags, index="fulltext"):
        """External method 'put' - puts data to Elasticsearch. Uses '__put'."""
        doc = {
            'key': key,
            'data': data,
            'date': date,
			'head': head,
            'tags': tags,
            'timestamp': datetime.datetime.now()
        }
        print(doc)
        self.__put(index, "all", key, doc)

    def __put(self, indexES, typeES, idES, dataES):
        """Internal method '__put' - puts data to Elasticsearch."""
        try:
            res = self.es.index(index=indexES, doc_type=typeES, id=idES, body=dataES, refresh=True)
            print(res)
            return res
        except exceptions as error:
            print("ERROR:"+error)
            raise error

    def search(self, index):
        """External method 'search' - searches data in Elasticsearch."""
        res = self.es.search(index=index)
        # print(res['hits']['hits'])
        parsed = json.loads(json.dumps(res['hits']['hits']))
        print(json.dumps(parsed, indent=4, sort_keys=True))
        return res

    def index(self, index):
        """External method 'index' - creates index in Elasticsearch."""
        try:
            res = self.es.indices.create(index=index)
            return res
        except exceptions.RequestError as error:
            if str(error).find("resource_already_exists_exception"):
                print("resource_already_exists_exception")
            else:
                raise error

    def delete(self, index):
        """External method 'delete' - deletes index in Elasticsearch."""
        self.es.indices.delete(index=index)

    def exists(self, index):
        """External method 'exists' - exists index in Elasticsearch."""
        res = self.es.exists(index)
        print(res['_source'])

    def get(self, index):
        """External method 'get' - gets index in Elasticsearch."""
        res = self.es.get(index=index)
        print(res['_source'])
        return res


if __name__ == "__main__":
    elasticsearchCrawlerClient = ElasticsearchCrawlerClient("http://172.26.7.84:9200/")
    elasticsearchCrawlerClient.put(key="https://malbaka.com/", data="Some data here!", date="2018.11.02", head="SuperHead", tags=["tag0", "tag1", "tag2"])
    elasticsearchCrawlerClient.contains(key="https://malbaka.com/")
    #elasticsearchCrawlerClient.search("_all")
    # elasticsearch.helpers.bulk(request, "index", stats_only=False)
    # elasticsearchCrawlerClient.delete("testindex1")
    # print(elasticsearchCrawlerClient.__dict__)
    # print(elasticsearchCrawlerClient.__dict__.keys())
    # print(elasticsearchCrawlerClient.__doc__)
