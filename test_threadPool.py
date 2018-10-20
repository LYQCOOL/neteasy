# -*- coding: utf-8 -*-
import requests
import threading
import Queue
import json
import time
import re
from Pipe import MongoDB, TempMongoDB
from lxml import etree
from datetime import datetime


class MyFetchThreadFirst(threading.Thread):
    def __init__(self, workQueue, saveQueue, timeout=30):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self.setDaemon(True)
        self.workQueue = workQueue
        self.saveQueue = saveQueue
        self.start()

    def working(self, item):
        # 返回所需要的内容,fetchFirst->json
        header = {
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Host': 'temp.163.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/59.0.3071.115 Safari/537.36'
        }
        while 1:
            try:
                temp_data = requests.get(url=item, headers=header, timeout=30)
                temp_data = temp_data.content.decode('gbk')
                temp_data = temp_data[14:-1]
                data = json.loads(temp_data, encoding='utf-8',strict=False)
                break
            except Exception as e:
                print "ERROR: Locate in the FetchFirst's working loop 'while 1', exception: %s" % e

        return data

    def run(self):
        try:
            while not self.workQueue.empty():
                # print "%s start working" % self.name
                item = self.workQueue.get()
                data = self.working(item)
                for one_data in data:
                    self.saveQueue.put(one_data)
        except Exception as e:
            print "ERROR: Locate in the FetchFirst's run method, exception: %s" % e


class MyFetchThreadSecond(threading.Thread):
    def __init__(self, workQueue, saveQueue, timeout=30):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self.setDaemon(True)
        self.workQueue = workQueue
        self.saveQueue = saveQueue
        self.mongodb = MongoDB()
        self.start()

    def working_one(self, item, label):
        # 返回所需要的内容,fetchFirst->json
        if label == 'shehui' or label == 'guonei':
            label = 'news.163.com'
        else:
            label = '%s.163.com' % label
        header = {
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Host': label,
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/59.0.3071.115 Safari/537.36'
        }
        flag = 1
        html = ''
        while 1:
            try:
                html = requests.get(url=item, headers=header, timeout=30). \
                    content.decode('gbk').encode('utf-8')
                break
            except Exception as e:
                print e
                flag += 1
            if flag > 10:
                break
        wenzhang_tree = etree.HTML(html, parser=etree.HTMLParser(encoding='utf-8'))
        return wenzhang_tree

    def Xpath_for_content(self, tree, path):
        try:
            content = tree.xpath(path)
            return content
        except Exception as e:
            print "ERROR: Locate in the FetchSecond's Xpath_for_content method, exception: e" % e
            return None

    def run(self):
        while not self.workQueue.empty():
            try:
                # print "%s start working" % self.name
                # condition : json's label should be 其他, json's time should be today
                item = self.workQueue.get()
                if item['label'] != u'其它':
                    continue
                current_time = time.strftime("%m/%d/%Y", time.localtime(time.time()))
                result = re.search(current_time, item['time'])
                if result == None:
                    continue
                # get new's html tree
                wenzhang_tree = self.working_one(item['docurl'], item['channelname'])
                message_dict = dict()
                # 文章来源
                wen_zhang_lai_yuan = self.Xpath_for_content(wenzhang_tree, '//*[@id="ne_article_source"]/text()')
                message_dict['wen_zhang_lai_yuan'] = wen_zhang_lai_yuan[0]
                # 文章正文
                wen_zhang_zheng_wen = self.Xpath_for_content(wenzhang_tree, '//*[@id="endText"]//p/text()')
                b = '\n'
                for temp in wen_zhang_zheng_wen:
                    b += temp
                message_dict['wen_zhang_zheng_wen'] = b
                # 文章栏目
                wen_zhang_lan_mu = self.Xpath_for_content(wenzhang_tree,
                                                          '//*[@id="ne_wrap"]/body//div/div[@class="clearfix'
                                                          '"]/div[@class="post_crumb"]//a/text()')
                c = '\n'
                for temp2 in wen_zhang_lan_mu:
                    c += temp2
                    c += ' '
                message_dict['wen_zhang_lan_mu'] = c
                # 评论网址
                ping_lun_wang_zhi = item['commenturl']
                message_dict['ping_lun_wang_zhi'] = ping_lun_wang_zhi
                # 文章网址
                wen_zhang_wang_zhi = item['docurl']
                message_dict['wen_zhang_wang_zhi'] = wen_zhang_wang_zhi
                # 文章标题
                wen_zhang_biao_ti = item['title']
                message_dict['wen_zhang_biao_ti'] = wen_zhang_biao_ti
                # 发布时间
                fa_bu_shi_jian = item['time']
                message_dict['fa_bu_shi_jian'] = fa_bu_shi_jian
                # 参与人数
                ping_lun_shu_liang = item['tienum']
                message_dict['ping_lun_shu_liang'] = ping_lun_shu_liang
                # 抓取时间
                do_time = time.time()
                message_dict['do_time'] = do_time
                # 抓取网站
                zhan_dian = u'网易新闻'
                message_dict['zhan_dian'] = zhan_dian
                # 图片链接
                tu_pian_lian_jie = None
                message_dict['tu_pian_lian_jie'] = tu_pian_lian_jie
                # 文章作者
                wen_zhang_zuo_zhe = None
                message_dict['wen_zhang_zuo_zhe'] = wen_zhang_zuo_zhe
                # 关键词
                try:
                    guan_jian_ci = item['keywords'][0]['keyname']
                    message_dict['guan_jian_ci'] = guan_jian_ci
                except Exception as e:
                    message_dict['guan_jian_ci'] = None
                    print "ERROR: Locate in the FetchSecond's run method for guan_jian_ci, exception: %s" % e
                # 相关标签
                xiang_guan_biao_qian = None
                message_dict['xiang_guan_biao_qian'] = xiang_guan_biao_qian
                # 阅读数量
                yue_du_shu = ping_lun_shu_liang
                message_dict['yue_du_shu'] = yue_du_shu
                # 主键
                message_dict['_id'] = wen_zhang_wang_zhi
                # save message_dict
                self.mongodb.put_content(message_dict)
                # some info
                url_info = re.match('https?://(.*?).163.com/\d*?/\d*?/\d*?/(.*?).html', wen_zhang_wang_zhi)
                all_thing = (url_info, wen_zhang_wang_zhi)
                self.saveQueue.put(all_thing)
            except Exception as e:
                print "ERROR: Locate in the FetchSecond's run method 'while not Queue empty', exception: %s" % e
                continue


class MyCommentThread(threading.Thread):
    def __init__(self, workqueue):
        threading.Thread.__init__(self)
        self.workQueue = workqueue
        self.setDaemon(True)
        self.start()
        self.mongodb = MongoDB()
        self.checkMongoDB = TempMongoDB()

    def run(self):
        while not self.workQueue.empty():
            try:
                # print "%s start working" % self.name
                info, wenzhang_Url = self.workQueue.get()
                default_url = 'http://comment.%s.163.com/api/v1/products/a2869674571f77b5a0867c3d71db5856/threads/%s/' \
                              'comments/newList?offset=0&limit=30&showLevelThreshold=72&headLimit=1&tailLimit=2' % \
                              (info.group(1), info.group(2))
                pages = self.working(wenzhang_Url, default_url, info)
                if pages > 0:
                    comment_urls = list()
                    for i in range(1, pages + 1):
                        offset = i * 30
                        temp = 'http://comment.%s.163.com/api/v1/products/a2869674571f77b5a0867c3d71db5856/threads/%s' \
                               '/comments/newList?offset=%d&limit=30&showLevelThreshold=72&headLimit=1&' \
                               'tailLimit=2' % (info.group(1), info.group(2), offset)
                        comment_urls.append(temp)
                    for item in comment_urls:
                        drop = self.working(wenzhang_Url, item, info)
            except Exception as e:
                print "ERROR: Locate in the CommentThread's run method 'while not Queue empty', exception: %s" % e
                continue

    def working(self, content_url, the_comment_url, info):
        host = 'comment.%s.163.com' % (info.group(1))
        referer = the_comment_url
        header = {
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Host': host,
            'Referer': referer,
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/59.0.3071.115 Safari/537.36'
        }
        # 默认获取第一页的json数据
        flag = 1
        while 1:
            try:
                json_data = json.loads(requests.get(url=the_comment_url, headers=header, timeout=30).content)
                break
            except Exception as e:
                print "ERROR: Failed to get the comment's json, exception: %s" % e
                flag += 1
            if flag > 5:
                return
        pages = 0
        try:
            for comment_id in json_data['comments']:
                comment_dict = dict()
                # 评论内容
                ping_lun_nei_rong = json_data['comments'][str(comment_id)]['content']
                comment_dict['ping_lun_nei_rong'] = ping_lun_nei_rong

                # 评论时间
                ping_lun_shi_jian = json_data['comments'][str(comment_id)]['createTime']
                comment_dict['ping_lun_shi_jian'] = ping_lun_shi_jian

                # 回复数量
                hui_fu_shu = None
                comment_dict['hui_fu_shu'] = hui_fu_shu

                # 点赞数量
                dian_zan_shu = json_data['comments'][str(comment_id)]['vote']
                comment_dict['dian_zan_shu'] = dian_zan_shu

                # 评论ID
                ping_lun_id = comment_id
                comment_dict['ping_lun_id'] = ping_lun_id

                # 用户昵称
                try:
                    yong_hu_ming = json_data['comments'][str(comment_id)]['user']['nickname']
                    comment_dict['yong_hu_ming'] = yong_hu_ming
                except Exception as e:
                    comment_dict['yong_hu_ming'] = None

                # 性别
                comment_dict['xing_bie'] = None

                # 用户等级
                comment_dict['yong_hu_deng_ji'] = None

                # 用户省份
                comment_dict['yong_hu_sheng_fen'] = json_data['comments'][str(comment_id)]['user']['location']

                # 抓取时间
                do_time = time.time()
                comment_dict['do_time'] = do_time

                # 抓取网站
                zhan_dian = u'网易新闻'
                comment_dict['zhan_dian'] = zhan_dian

                # 主键
                comment_dict['_id'] = ping_lun_id + content_url

                # 获取评论数
                ping_lun_shu = json_data['newListSize']
                pages = ping_lun_shu / 30

                self.mongodb.put_comment(comment_dict)
                # put the data into the database
                check_dict = dict()
                check_dict['_id'] = content_url
                check_dict['do_time'] = do_time
                check_dict['ping_lun_shu'] = ping_lun_shu
                self.checkMongoDB.put(check_dict)
            return pages
        except Exception as e:
            print "ERROR: Locate in the CommentThread's working method for parsing json data, exception: %s," \
                  "and json data is %s" % (e, json_data)


class CheckUpdate(object):
    def __init__(self, savequeue):
        self.saveQueue = savequeue
        self.check_mongodb = TempMongoDB()
        self.update_mongodb = MongoDB()

    def run(self):
        old_data = self.check_mongodb.get()
        count_for_news = 0
        count_for_comment = 0
        try:
            for every_data in old_data:
                ping_lun_shu = every_data['ping_lun_shu']
                content_url = every_data['_id']
                # get comment's num and then compare,if the num has been changed,then get the new data
                info = re.match('http://(.*?).163.com/\d*?/\d*?/\d*?/(.*?).html', content_url)
                default_url = 'http://comment.%s.163.com/api/v1/products/a2869674571f77b5a0867c3d71db5856/threads/%s/' \
                              'comments/newList?offset=0&limit=30&showLevelThreshold=72&headLimit=1&tailLimit=2' % \
                              (info.group(1), info.group(2))
                current_shuliang = self.working(info=info, comment_url=default_url)['newListSize']
                num = current_shuliang - ping_lun_shu
                if num > 0:
                    put_data = (info, content_url)
                    condition_one = {'_id': content_url}
                    condition_two = {'do_time': time.time()}
                    self.saveQueue.put(put_data)
                    self.update_mongodb.update_content(condition_one, condition_two)
                    count_for_news += 1
                    count_for_comment += num
                else:
                    remove_condition = {
                        "_id": content_url
                    }
                    self.check_mongodb.delete(remove_condition)
        except Exception as e:
            print "ERROR: Locate in the CheckUpdate, exception: %s" % e
        finally:
            print "UPDATE: There are %d news has been updated, and there has %d comments been updated" % \
                  (count_for_news, count_for_comment)

    def working(self, info, comment_url):
        host = 'comment.%s.163.com' % (info.group(1))
        referer = comment_url
        header = {
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Host': host,
            'Referer': referer,
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/59.0.3071.115 Safari/537.36'
        }
        flag = 1
        while 1:
            try:
                json_data = json.loads(requests.get(url=comment_url, headers=header, timeout=30).content)
                break
            except Exception as e:
                print "ERROR: Failed to get the comment's json, exception: %s" % e
                flag += 1
            if flag > 5:
                return
        return json_data


class ThreadPoolOfFetchSecond(object):
    def __init__(self, num_of_threads, workqueue, savequeue):
        self.workQueue = workqueue
        self.saveQueue = savequeue
        self.threads = []
        self.createThreadPool(num_of_threads)

    def createThreadPool(self, num_of_threads):
        for item in range(num_of_threads):
            thread = MyFetchThreadSecond(workQueue=self.workQueue, saveQueue=self.saveQueue)
            self.threads.append(thread)

    def wait_for_complete(self):
        while len(self.threads):
            thread = self.threads.pop()
            if thread.isAlive():
                thread.join()


class ThreadPoolOfCommentThread(object):
    def __init__(self, num_of_threads, workqueue):
        self.workQueue = workqueue
        self.threads = []
        self.createThreadPool(num_of_threads)

    def createThreadPool(self, num_of_threads):
        for item in range(num_of_threads):
            thread = MyCommentThread(workqueue=self.workQueue)
            self.threads.append(thread)

    def wait_for_complete(self):
        while len(self.threads):
            thread = self.threads.pop()
            if thread.isAlive():
                thread.join()


def DeltaSeconds():
    curTime = datetime.now()
    try:
        desTime = curTime.replace(day=curTime.day+1, hour=20, minute=30, second=0, microsecond=0)
    except ValueError:
        desTime = curTime.replace(month=curTime.month+1, day=1, hour=20, minute=30, second=0, microsecond=0)
    delta = desTime - curTime
    skipSeconds = delta.total_seconds()
    return skipSeconds


def getCondition():
    curTime = datetime.now()
    start_Time = curTime.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        end_Time = curTime.replace(day=start_Time.day+1, hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        end_Time = curTime.replace(month=curTime.month+1, day=1, hour=0, minute=0, second=0, microsecond=0)
    timeArrayOne = time.strptime(str(start_Time), "%Y-%m-%d %H:%M:%S")
    timeArrayTwo = time.strptime(str(end_Time), "%Y-%m-%d %H:%M:%S")
    timeStamp = float(time.mktime(timeArrayOne))
    timeStamp2 = float(time.mktime(timeArrayTwo))
    return timeStamp, timeStamp2


if __name__ == '__main__':
    count = 0
    while True:
        start_moment = time.time()
        count += 1
        current_date = time.time()
        local_date = time.localtime(current_date)
        format_date = time.strftime("%Y-%m-%d-%H-%M", local_date)
        print "RUNTIME: This is %d running time, and Date is %s now." % (count, format_date)
        # news' url
        Urls = ['http://temp.163.com/special/00804KVA/cm_shehui.js?callback=data_callback',
                'http://temp.163.com/special/00804KVA/cm_guonei.js?callback=data_callback',
                'http://temp.163.com/special/00804KVA/cm_ent.js?callback=data_callback',
                'http://temp.163.com/special/00804KVA/cm_money.js?callback=data_callback',
                'http://temp.163.com/special/00804KVA/cm_sports.js?callback=data_callback']
        # all queues
        StartUrlQueue = Queue.Queue()
        Json_Url_Queue = Queue.Queue()
        urlInfo_Queue = Queue.Queue()
        newsCheck_Queue = Queue.Queue()
        updateUrl_Queue = Queue.Queue()
        # First: check old news' url for update
        Updater = CheckUpdate(savequeue=updateUrl_Queue)
        Updater.run()
        CommentPool = ThreadPoolOfCommentThread(num_of_threads=10, workqueue=updateUrl_Queue)
        CommentPool.wait_for_complete()
        print "UPDATE: Complete checking for the old news!"
        # Second: Get all news' url for json
        for url in Urls:
            StartUrlQueue.put(url)
        fetch_1 = MyFetchThreadFirst(workQueue=StartUrlQueue, saveQueue=Json_Url_Queue)
        fetch_1.join()
        # Third: Parse json data and get news' html and parse to generate the message_dict then save it
        print "FETCH: There are %d newest news has been fetched today" % Json_Url_Queue.qsize()
        FetchSecondPool = ThreadPoolOfFetchSecond(num_of_threads=6, workqueue=Json_Url_Queue, savequeue=urlInfo_Queue)
        FetchSecondPool.wait_for_complete()
        # Forth: Request the comment_url and save comment's message_dict
        CommentPool = ThreadPoolOfCommentThread(num_of_threads=10, workqueue=urlInfo_Queue)
        CommentPool.wait_for_complete()
        start_uptime, end_uptime = getCondition()
        time_condition = {"do_time": {"$gte": start_uptime, "$lt": end_uptime},
                          "zhan_dian": "网易新闻"}
        temp_mongo = MongoDB()
        num_of_comments = temp_mongo.get_comment_data(time_condition)
        print "FETCH: There are %d comments has been fetched today" % num_of_comments
        print "TIME: Total spent %d seconds" % (time.time() - start_moment)
        print "SLEEP: Mission complete, start to sleeping.... "
        sleep_seconds = DeltaSeconds()
        time.sleep(sleep_seconds)

