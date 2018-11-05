from elasticsearch import exceptions
import ElasticsearchCrawlerClient
from elasticsearch.connection import create_ssl_context
import time
import datetime
import json
import sys


class DocumentStateManager(object):
    """DocumentStateManager is a CrawlerClient for Elasticsearch."""

    def __init__(self, ElasticsearchCrawlerClient, BaseIndex="bufferindex"):
        """DocumentStateManager Constructor"""
        self.base_index = BaseIndex
        self.client = ElasticsearchCrawlerClient
        self.document = self.__get_document(self.client, self.base_index)
        if self.document != None:
            self.id = self.document['key']
        else:
            self.id = None

    def __get_document(self, ElasticsearchCrawlerClient, index):
        """Internal method '__get' - get id in index in Elasticsearch."""
        try:
            res = ElasticsearchCrawlerClient.search(index=index, doc_type="all", size=1)
            if res['hits']['total'] != 0:
                print("Not None")
                parsed_result = res['hits']['hits'][0]['_source']
                print(json.dumps(json.loads(json.dumps(parsed_result)), indent=4, sort_keys=True))
                return parsed_result
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

    def change_state(self, NewIndex):
        """External method 'ChangeState' - deletes id in index in Elasticsearch and creates id in NewIndex."""
        try:
            print("ChangeState")
            self.client.buffer_put(index=NewIndex, doc_type="all", key=self.id, body=self.document)
            print("DeleteInBuffer")
            self.client.delete_document(index=self.base_index, doc_type="all", id=self.id)
            return True
        except exceptions as error:
            print(error)
            raise error


if __name__ == "__main__":
    print(sys.version)

    Client = ElasticsearchCrawlerClient.ElasticsearchCrawlerClient("http://172.26.7.84:9200/")
<<<<<<< HEAD
    DocumentStateManager = DocumentStateManager(Client, "bufferindex")
    if DocumentStateManager.document != None:
        print(DocumentStateManager.document)
        DocumentStateManager.change_state("newIndex")
=======
    Manager = DocumentStateManager(Client, "bufferindex")
    if Manager.document != None:
        print(Manager.document)
        Manager.change_state(Client, "newIndex")
>>>>>>> 24dfe1b30ec355869d83c971603029644c4f63ac
