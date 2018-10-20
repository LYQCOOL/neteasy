# coding: utf-8

import pymongo


class MongoDB(object):
    def __init__(self):
        # client = pymongo.MongoClient('182.150.37.55', 50070)
        client = pymongo.MongoClient()
        # client = pymongo.MongoClient('localhost', 27017)
        db = client['news']
        self.collection_zhengwen = db['content']
        self.collection_comment = db['comment']

    def put_content(self, value):
        return self.collection_zhengwen.save(value)

    def put_comment(self, value):
        return self.collection_comment.save(value)

    def update_content(self, condition1, condition2):
        return self.collection_zhengwen.update_one(condition1, {'$set': condition2})

    def get_comment_data(self, condition):
        return self.collection_comment.find(condition).count()


class TempMongoDB(object):
    def __init__(self):
        # client = pymongo.MongoClient('182.150.37.55', 50070)
        # client = pymongo.MongoClient('localhost', 50070)
        client = pymongo.MongoClient('localhost', 27017)
        db = client['NetEasy_temp']
        self.collection = db['news_check']

    def put(self, value):
        return self.collection.save(value)

    def get(self, condition=None):
        return self.collection.find(condition)

    def delete(self, condition=None):
        return self.collection.remove(condition)
