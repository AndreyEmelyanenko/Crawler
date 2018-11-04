from elasticsearch import Elasticsearch
from elasticsearch import exceptions
from elasticsearch.connection import create_ssl_context
import time
import datetime
import json
import sys


class DocumentStateManager(object):
    """DocumentStateManager is a CrawlerClient for Elasticsearch."""

    def __init__(self, url):
        """DocumentStateManager Constructor"""
        self.es = Elasticsearch(url)
        self.bufferindex = str("bufferindex")
        self.document = self.__get()
        if self.document != None:
            print("OK")
            self.id = self.document['key']
        else:
            print("NOT OK")
            self.id = None

    def __get(self):
        """Internal method '__get' - get id in index in Elasticsearch."""
        try:
            res = self.es.search(index=self.bufferindex, doc_type="all", size=1)
            print(res)
            if res['hits']['total'] != 0:
                print("Not None")
                resparsed = res['hits']['hits'][0]['_source']
                print(json.dumps(json.loads(json.dumps(resparsed)), indent=4, sort_keys=True))
                return resparsed
            else:
                print("None")
                return None
        except exceptions.NotFoundError as error:
            print(error)
            if error.status_code == 404:
                return None
            else:
                raise error
        except exceptions.__all__ as error:
            raise error

    def ChangeState(self, NewIndex):
        """External method 'ChangeState' - deletes id in index in Elasticsearch and creates id in NewIndex."""
        try:
            print("ChangeState")
            res = self.es.index(index=NewIndex, doc_type="all", id=self.id, body=self.document, refresh=True)
            print("delete")
            self.es.delete(index=self.bufferindex, doc_type="all", id=self.id, refresh=True)
            print(res)
            return res
        except exceptions as error:
            print(error)
            raise error

    def Search(self, index):
        """External method 'search' - searches data in Elasticsearch."""
        print("Search")
        res = self.es.search(index=index)
        parsed = json.loads(json.dumps(res))
        print(json.dumps(parsed, indent=4, sort_keys=True))
        return res


if __name__ == "__main__":
    print(sys.version)
    DocumentStateManager = DocumentStateManager("http://172.26.7.84:9200/")
    if DocumentStateManager.document != None:
        print(DocumentStateManager.document)
    if DocumentStateManager.id != None:
        print(DocumentStateManager.id)

    if DocumentStateManager.document != None:
        DocumentStateManager.ChangeState("testindex4")
        DocumentStateManager.Search("bufferindex")
        DocumentStateManager.Search("testindex4")
