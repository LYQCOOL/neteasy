# coding=utf-8
import pymongo
from datetime import datetime
import time

def getcondition():
    curTime = datetime.now()
    currentday = curTime.day
    start_Time = curTime.replace(day=currentday-1,hour=0, minute=0, second=0, microsecond=0)
    try:
        end_Time = curTime.replace(day=start_Time.day+1, hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        end_Time = curTime.replace(month=curTime.month+1, day=1, hour=0, minute=0, second=0, microsecond=0)
    timeArrayOne = time.strptime(str(start_Time), "%Y-%m-%d %H:%M:%S")
    timeArrayTwo = time.strptime(str(end_Time), "%Y-%m-%d %H:%M:%S")
    timeStamp = float(time.mktime(timeArrayOne))
    timeStamp2 = float(time.mktime(timeArrayTwo))
    return timeStamp, timeStamp2

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
    
    def get_one_content(self, condition):
        return self.collection_zhengwen.find_one(condition)


if __name__ == '__main__':
    start_uptime,end_uptime = getcondition()
    time_condition = {"do_time": {"$gte": start_uptime, "$lt": end_uptime},
                          "zhan_dian": "新浪网"}
    mongo = MongoDB()
    content_data = mongo.get_one_content(time_condition)
    print content_data 
