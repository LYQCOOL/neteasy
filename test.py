# _*_ encoding:utf-8 _*_
__author__ = 'LYQ'
__date__ = '2018/10/16 21:46'
import re
test='http://news.163.com/18/1016/21/DU95VRSH0001875P.html'
print re.match('https?://(.*?).163.com/\d*?/\d*?/\d*?/(.*?).html',test).groups()
