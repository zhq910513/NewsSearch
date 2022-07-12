#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

import random

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import sys
from urllib.parse import urlencode

sys.path.append("../")
import configparser
import logging
import pprint
import time
import re

import os
from os import path

import requests
from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/pe_wangye.log'))
settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))

if not os.path.isfile(logPath):
    open(logPath, 'w+')

logger = logging.getLogger(logPath)
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# 读ini文件
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")


# 搜索规则 ：每次启动从数据库获取categoryUrl，结束时间自动设置为当天


class PE:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")

        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=datadb))

        cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        self.message_coll = client[datadb]['pe_wangye_messages']
        self.articleData_coll = client[datadb]['pe_wangye_articleData']
        self.pp_articleData_coll = client[datadb]['pp_wangye_articleData']
        self.userAgent = UserAgent().random
        self.pageApiUrl_gn = 'https://plas.chem99.com/news/?page={page}&sid=9978&siteid=3'
        self.ZCHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=9978&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
        }
        self.LZHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'list.oilchem.net',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.JLCHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'plas.315i.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.ZCarticleHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/37283292.html',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'connection': 'close',
            'referer': 'https://plas.chem99.com/news/37283292.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.LZarticleHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'close',
            'Host': 'news.oilchem.net',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.JLCarticleHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'plas.315i.com',
            'Pragma': 'no-cache',
            'Referer': 'http://plas.315i.com/infodetail/i14276534_p004001001_c005010.html',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.juBingXiApi = 'http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html'
        self.juBingXiHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '67',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'www.dce.com.cn',
            'Origin': 'http://www.dce.com.cn',
            'Pragma': 'no-cache',
            'Referer': 'http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.titleOne = ''
        self.titleTwo = ''
        self.titleThree = ''

    """
        国内PE装置动态汇总 / 国际装置投产及检修计划汇总 / 聚乙烯企业开工率跟踪报道 / 石化库存PE+PP / PE包装膜
    """

    def GetAllMessages(self, info, history=False, pageNum=1):
        print('第 {} 页'.format(pageNum))
        link = info.get('url').format(page=pageNum)
        zc_api = 'https://www.sci99.com/search/ajax.aspx'

        if 'plas.chem99.com' in link:
            headers = {
                'authority': 'www.sci99.com',
                'method': 'POST',
                'path': '/search/ajax.aspx',
                'scheme': 'https',
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'content-length': '149',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'cookie': 'guid=87ffc1de-3585-c12a-dbed-616c9c4f2d5b; UM_distinctid=17587ca66304c-0f0d4668921dcc-7a1b34-ffc00-17587ca6631b4a; STATReferrerIndexId=1; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; isCloseOrderZHLayer=0; route=20f9b27d1afe4e6e9c894ec30494ada5; ASP.NET_SessionId=rpczfeyti4d0oxoivknmq5g4; CNZZDATA1269807659=707844950-1604300347-https%253A%252F%252Fwww.sci99.com%252F%7C1619670242; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1617951832,1619413702,1619670158,1619673064; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fwww.sci99.com%2F; Hm_lpvt_44c27e8e603ca3b625b6b1e9c35d712d=1619673082; pageViewNum=3',
                'origin': 'https://www.sci99.com',
                'pragma': 'no-cache',
                'referer': 'https://www.sci99.com/search/?key=%E5%9B%BD%E5%86%85PE%E8%A3%85%E7%BD%AE%E5%8A%A8%E6%80%81%E6%B1%87%E6%80%BB',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest'
            }
            jsonData = {
                'action': 'getlist',
                'keyword': info.get('Type'),
                'sccid': 0,
                'pageindex': pageNum,
                'siteids': 0,
                'pubdate': '',
                'orderby': 'true'
            }
            try:
                resp = requests.post(url=zc_api, headers=headers, data=urlencode(jsonData), timeout=5, verify=False)

                resp.encoding = 'utf-8'
                if resp.status_code == 200:
                    data = self.ParseMessages(info, resp.json()[0])
                    if data.get('dataList'):
                        for msg in data.get('dataList'):
                            msg.update({'Type': info.get('Type')})
                            print(msg)

                            self.message_coll.update_one({'link': msg['link']}, {'$set': msg}, upsert=True)
                    else:
                        pass

                    if history:
                        if pageNum <= data.get('maxPage'):
                            time.sleep(3)
                            return self.GetAllMessages(info, history, pageNum + 1)
                        else:
                            pass
                    else:
                        todayStatus = False
                        for msg in data.get('dataList'):
                            msg.update({'Type': info.get('Type')})

                            self.message_coll.update_one({'link': msg['link']}, {'$set': msg}, upsert=True)

                            if str(time.strftime("%Y-%m-%d", time.localtime(time.time()))) in msg.get('uploadTime'):
                                todayStatus = True

                        if todayStatus:
                            time.sleep(3)
                            return self.GetAllMessages(info, history, pageNum + 1)
                        else:
                            pass
                else:
                    print(resp.status_code)
            except TimeoutError:
                logger.warning(link)
            except Exception as error:
                logger.warning(error)
                return
        else:
            if 'list.oilchem.net' in link:
                headers = self.LZHeaders
            else:
                headers = self.JLCHeaders

            try:
                resp = requests.get(url=link, headers=headers, timeout=5, verify=False)
                resp.encoding = 'utf-8'
                if resp.status_code == 200:
                    data = self.ParseMessages(info, resp.text)
                    # print(data)
                    if data.get('maxPage'):
                        if history:
                            if data.get('dataList'):
                                for msg in data.get('dataList'):
                                    msg.update({'Type': info.get('Type')})
                                    print(msg)

                                    self.message_coll.update_one({'link': msg['link']}, {'$set': msg}, upsert=True)

                            else:
                                pass

                            if pageNum <= data.get('maxPage'):
                                time.sleep(3)
                                return self.GetAllMessages(info, history, pageNum + 1)
                            else:
                                pass
                        else:
                            todayStatus = False
                            for msg in data.get('dataList'):
                                msg.update({'Type': info.get('Type')})

                                self.message_coll.update_one({'link': msg['link']}, {'$set': msg}, upsert=True)

                                if str(time.strftime("%Y-%m-%d", time.localtime(time.time()))) in msg.get('uploadTime'):
                                    todayStatus = True

                            if todayStatus:
                                time.sleep(3)
                                return self.GetAllMessages(info, history, pageNum + 1)
                            else:
                                pass
                    else:
                        pass
                else:
                    print(resp.status_code)
            except TimeoutError:
                logger.warning(link)
            except Exception as error:
                logger.warning(error)
                return

    @staticmethod
    def ParseMessages(info, Html):
        maxPage = 0
        if isinstance(Html, str):
            soup = BeautifulSoup(Html, 'lxml')
            data = {}
            dataList = []
            if '国际装置投产及检修计划汇总' in info.get('Type') or '聚乙烯企业开工率跟踪报道' in info.get('Type') or '生产企业库存早报' in info.get(
                    'Type') or '主要港口库存动态' in info.get('Type') or 'PE包装膜企业开工率' in info.get('Type'):
                try:
                    if soup.find_all('li', {'class': 'clearfix'}):
                        for li in soup.find_all('li', {'class': 'clearfix'}):
                            # title
                            try:
                                title = li.find('a').get_text().replace('\n', '').replace('\t', '').replace('\r',
                                                                                                            '').strip()
                            except:
                                title = None

                            # link
                            try:
                                link = li.find('a').get('href')
                            except:
                                link = None

                            # 时间
                            try:
                                uploadTime = li.find('span').get_text().replace('\n', '').replace('\t', '').replace(
                                    '\r',
                                    '').strip().replace(
                                    '  ', ' ').replace('                                                  ',
                                                       ' ').replace(
                                    '                                                ', ' ')
                            except:
                                uploadTime = None

                            if info.get('Type') in title:
                                if link:
                                    dataList.append({
                                        'title': title,
                                        'link': link,
                                        'uploadTime': uploadTime
                                    })
                                else:
                                    pass
                            else:
                                pass
                except Exception as error:
                    logger.warning(error)

                try:
                    if soup.find('ul', {'class': 'pages'}):
                        for a in soup.find('ul', {'class': 'pages'}).find_all('a'):
                            if '末页' in a.get_text():
                                maxPage = a.get('href').split('/')[-1].split('.')[0]
                                break
                            else:
                                maxPage = None
                    else:
                        maxPage = None
                except Exception as error:
                    logger.warning(error)
                    maxPage = None
            elif '进出口数据' in info.get('Type'):
                try:
                    if soup.find('div', {'class': 'pad019'}).find_all('li'):
                        for li in soup.find('div', {'class': 'pad019'}).find_all('li'):
                            # title
                            try:
                                title = li.find_all('a')[-1].get_text().replace('\n', '').replace('\t', '').replace(
                                    '\r',
                                    '').strip()
                            except:
                                title = None

                            # link
                            try:
                                link = 'http://plas.315i.com' + li.find_all('a')[-1].get('href')
                            except:
                                link = None

                            # 时间
                            try:
                                uploadTime = li.find('span', {'class': 'fr'}).get_text().replace('\n', '').replace('\t',
                                                                                                                   '').replace(
                                    '\r',
                                    '').strip().replace(
                                    '  ', ' ').replace('                                                  ', ' ')
                            except:
                                uploadTime = None

                            if '按产销国' in title or '按贸易方式' in title:
                                if link:
                                    dataList.append({
                                        'title': title,
                                        'link': link,
                                        'uploadTime': uploadTime
                                    })
                                else:
                                    pass
                except Exception as error:
                    logger.warning(error)

                try:
                    if soup.find('div', {'id': 'page'}):
                        for a in soup.find('div', {'id': 'page'}).find_all('a'):
                            if '共' in a.get_text():
                                maxPage = int(a.get_text().strip().split('条')[0].split('共')[1]) / 20
                                break
                            else:
                                maxPage = None
                    else:
                        maxPage = None
                except Exception as error:
                    logger.warning(error)
                    maxPage = None
            else:
                print('没有该类型 -- {}'.format(info.get('Type')))

            if maxPage:
                data.update({
                    'dataList': dataList,
                    'maxPage': int(maxPage)
                })
            if data:
                return data

        if isinstance(Html, dict):
            data = {}
            dataList = []

            if Html.get('hits'):
                for item in Html.get('hits'):
                    # title
                    try:
                        title = item.get('Title').replace('\n', '').replace('\t', '').replace('\r', '').strip()
                    except:
                        title = None

                    # link
                    try:
                        link = 'https://plas.chem99.com/news/{}.html'.format(item.get('NewsKey'))
                    except:
                        link = None

                    # 时间
                    try:
                        uploadTime = item.get('PubTime').replace('T', ' ')
                    except:
                        uploadTime = None

                    if info.get('Type') in title:
                        if link:
                            dataList.append({
                                'title': title,
                                'link': link,
                                'uploadTime': uploadTime
                            })
                        else:
                            pass
                    else:
                        pass

            if dataList:
                data.update({
                    'dataList': dataList,
                    'maxPage': int(Html.get('totalPages'))
                })
            if data:
                return data

    # 价格价差 --- 聚乙烯
    def GetSelectDate(self, history=False, timeStrf=str(time.strftime("%Y-%m-%d", time.localtime(time.time()))),
                      count=0):

        print('正在加载 %s 的数据...' % timeStrf)

        timeStamp = int(time.mktime(time.strptime(timeStrf, "%Y-%m-%d")))

        year = timeStrf.split('-')[0]
        month = str(int(timeStrf.split('-')[1]) - 1)
        day = timeStrf.split('-')[2]
        date = {
            'year': year,
            'month': month,
            'day': day
        }

        if not history:
            return self.GetJuYiXi(date)
        else:
            time.sleep(5)
            lastDayStamp = timeStamp - 86400
            lastDayStrf = str(time.strftime("%Y-%m-%d", time.localtime(lastDayStamp)))

            if count <= 7:
                year = lastDayStrf.split('-')[0]
                month = str(int(lastDayStrf.split('-')[1]) - 1)
                day = lastDayStrf.split('-')[2]

                self.GetJuYiXi({
                    'year': year,
                    'month': month,
                    'day': day,
                })

                return self.GetSelectDate(history, lastDayStrf, count + 1)

    def GetJuYiXi(self, selectDate: dict):
        try:
            year = selectDate.get('year')
            month = selectDate.get('month')
            day = selectDate.get('day')
            jsonData = {
                'dayQuotes.variety': 'l',
                'dayQuotes.trade_type': 0,
                'year': year,
                'month': month,
                'day': day
            }
            self.juBingXiHeaders.update({
                'Cookie': self.cookie_coll.find_one({'name': 'pe_juyixi'}).get('cookie')
            })

            resp = requests.post(url=self.juBingXiApi, headers=self.juBingXiHeaders, data=urlencode(jsonData),
                                 timeout=5, verify=False)
            resp.encoding = 'utf-8'

            if resp.status_code == 200:
                dataList = self.PaserJuYiXi(resp.text)
                if dataList:
                    jsonData.update({
                        'Type': '价格价差',
                        'hashKey': year + str(int(month) + 1) + day,
                        'tableData': dataList,
                        'month': str(int(month) + 1)
                    })
                    print(jsonData)
                    try:
                        self.articleData_coll.update_one({'hashKey': year + str(int(month) + 1) + day},
                                                         {'$set': jsonData}, upsert=True)
                    except Exception as error:
                        logger.warning(error)
                else:
                    print('没有数据！')
        except requests.exceptions.ConnectionError:
            return self.GetJuYiXi(selectDate)
        except Exception as error:
            logger.warning(error)

    @staticmethod
    def PaserJuYiXi(Html):
        soup = BeautifulSoup(Html, 'lxml')
        dataList = []

        try:
            # title
            titles = [th.get_text().strip() for th in
                      soup.find('div', {'id': 'printData'}).find_all('tr')[0].find_all('th')]

            for tr in soup.find('div', {'id': 'printData'}).find_all('tr')[1:]:
                try:
                    data = {}
                    info = tr.find_all('td')
                    if len(info) == len(titles):
                        if info[1].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip() in ['2101',
                                                                                                                '2105',
                                                                                                                '2109']:
                            for num in [0, 1, 7]:
                                data.update({
                                    titles[num]: info[num].get_text().replace('\n', '').replace('\t', '').replace('\r',
                                                                                                                  '').strip()
                                })
                            if data:
                                dataList.append(data)
                    else:
                        print('title 结构发生变化！')
                except:
                    pass
        except Exception as error:
            logger.warning(error)

        if dataList:
            return dataList

    """
        获取详细文章内容
    """

    def GetUrlFromMongo(self, info):
        print(info)
        link = info['link']
        print(link)

        if 'plas.chem99.com' in link:
            if info['Type'] == '国内PE装置动态汇总':
                self.ZCarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'zc_pe_装置动态_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '国内石化PE生产比例汇总':
                self.ZCarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'zc_pe_国内石化_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '农膜日评':
                self.ZCarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'zc_pe_农膜日评_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '塑膜收盘价格表':
                self.ZCarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'zc_pe_塑膜收盘_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '神华PE竞拍':
                self.ZCarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'zc_pe_神华竞拍_article'}).get('cookie'),
                     'referer': link})
            else:
                pass
            headers = self.ZCarticleHeaders
        elif 'oilchem.net' in link:
            proxy = False
            if info['Type'] == '国际装置投产及检修计划汇总':
                self.LZarticleHeaders = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Cookie': self.cookie_coll.find_one({'name': 'lz_pe_国际装置_article'}).get('cookie'),
                    'Host': 'www.oilchem.net',
                    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Microsoft Edge";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36 Edg/99.0.1150.46'
                }
            elif info['Type'] == '聚乙烯企业开工率跟踪报道':
                self.LZarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'lz_pe_聚乙烯开工率_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '生产企业库存早报':
                self.LZarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'lz_pe_企业库存_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '主要港口库存动态':
                self.LZarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'lz_pe_港口库存_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == 'PE包装膜企业开工率':
                self.LZarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'lz_pe_包装膜开工率_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == 'PE国内企业装置检修':
                self.LZarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'lz_pe_qiye_article'}).get('cookie'),
                     'referer': link})
            elif info['Type'] == '国内PP装置检修':
                self.LZarticleHeaders = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Cookie': self.cookie_coll.find_one({'name': 'lz_pp_gn_article'}).get('cookie'),
                    'Host': 'www.oilchem.net',
                    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Microsoft Edge";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36 Edg/99.0.1150.46'
                }
            else:
                pass
            headers = self.LZarticleHeaders
        elif 'plas.315i.com' in link:
            if info['Type'] == '进出口数据':
                self.JLCarticleHeaders.update(
                    {'cookie': self.cookie_coll.find_one({'name': 'jlc_pe_进出口数据_article'}).get('cookie'),
                     'referer': link})
            headers = self.JLCarticleHeaders
        else:
            headers = None
            print('新增平台 -- {}'.format(link.split('/')[2]))

        try:
            resp = requests.get(url=link, headers=headers, timeout=5, verify=False)

            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                dataList = self.ParseArticle(resp.text, info, link)
                if dataList:
                    if dataList == 1:
                        print('数据获取成功')
                    else:
                        print(dataList)
                        try:
                            self.articleData_coll.update_one({'link': link}, {'$set': {
                                'link': link,
                                'Type': info['Type'],
                                'title': info['title'],
                                'uploadTime': info['uploadTime'].replace(
                                    '                                                ',
                                    ' '),
                                'dataList': dataList
                            }}, upsert=True)
                        except Exception as error:
                            logger.warning(error)

                        self.message_coll.update_one({'link': link}, {'$set': {'status': 1}}, upsert=True)
                else:
                    print('没有数据!')
            else:
                self.message_coll.delete_one({'link': link})
                print('{} -- 服务器访问太频繁'.format(resp.status_code))
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetUrlFromMongo(info)
        except TimeoutError:
            print('网络问题，重试中...')
            return self.GetUrlFromMongo(info)
        except Exception as error:
            logger.warning(error)
            return

        time.sleep(random.uniform(15, 20))

    def ParseArticle(self, Html, info, link):
        soup = BeautifulSoup(Html, 'lxml')
        dataList = []

        if info.get('Type') == '国内PE装置动态汇总':
            try:
                if soup.find('tbody'):
                    if '未找到您的权限信息' in soup.find('span', {'id': 'zoom'}).get_text():
                        self.message_coll.update_one({'link': link}, {'$set': {'status': '无权限'}}, upsert=True)
                        print('未找到您的权限信息！')
                        return
                    else:
                        # 表头
                        titles = [td.get_text().strip() for td in soup.find('tbody').find_all('tr')[0].find_all('td')]
                        for tr in soup.find('tbody').find_all('tr')[1:]:
                            data = {}
                            try:
                                if len(tr.find_all('td')) == len(titles):
                                    self.titleOne = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                        '\r', '').strip()
                                    self.titleTwo = tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                        '\r', '').strip()
                                    self.titleThree = tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                                '').replace(
                                        '\r', '').strip()

                                    for num in range(len(titles)):
                                        # if tr.find_all('td')[num].find('span'):
                                        #     key = titles[num]
                                        #     value = [tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()]
                                        #     if '停车' in value[0]:
                                        #         value = 0
                                        #     data.update({
                                        #         key: value
                                        #     })
                                        # else:
                                        key = titles[num]
                                        value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                            '\r', '').strip()
                                        # if '停车' in value:
                                        #     value = 0
                                        data.update({
                                            key: value
                                        })
                                elif len(tr.find_all('td')) + 1 == len(titles):
                                    self.titleTwo = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                        '\r', '').strip()
                                    self.titleThree = tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                                '').replace(
                                        '\r', '').strip()
                                    data.update({
                                        titles[0]: self.titleOne
                                    })
                                    for num in range(len(tr.find_all('td'))):
                                        # if tr.find_all('td')[num].find('span'):
                                        #     key = titles[num + 1]
                                        #     value = [tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()]
                                        #     if '停车' in value[0]:
                                        #         value = 0
                                        #     data.update({
                                        #         key: value
                                        #     })
                                        # else:
                                        key = titles[num + 1]
                                        value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                            '\r', '').strip()
                                        # if '停车' in value:
                                        #     value = 0
                                        data.update({
                                            key: value
                                        })
                                elif len(tr.find_all('td')) + 2 == len(titles):
                                    self.titleThree = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                                '').replace(
                                        '\r', '').strip()
                                    data.update({
                                        titles[0]: self.titleOne,
                                        titles[1]: self.titleTwo
                                    })
                                    for num in range(len(tr.find_all('td'))):
                                        # if tr.find_all('td')[num].find('span'):
                                        #     key = titles[num + 2]
                                        #     value = [tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()]
                                        #     if '停车' in value[0]:
                                        #         value = 0
                                        #     data.update({
                                        #         key: value
                                        #     })
                                        # else:
                                        key = titles[num + 2]
                                        value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                            '\r', '').strip()
                                        # if '停车' in value:
                                        #     value = 0
                                        data.update({
                                            key: value
                                        })
                                elif len(tr.find_all('td')) + 3 == len(titles):
                                    data.update({
                                        titles[0]: self.titleOne,
                                        titles[1]: self.titleTwo,
                                        titles[2]: self.titleThree
                                    })
                                    for num in range(len(tr.find_all('td'))):
                                        # if tr.find_all('td')[num].find('span'):
                                        #     key = titles[num + 3]
                                        #     value = [
                                        #         tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()]
                                        #     if '停车' in value[0]:
                                        #         value = 0
                                        #     data.update({
                                        #         key: value
                                        #     })
                                        # else:
                                        key = titles[num + 3]
                                        value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                            '\r', '').strip()
                                        # if '停车' in value:
                                        #     value = 0
                                        data.update({
                                            key: value
                                        })
                                else:
                                    logger.warning('表头对不上！  {}'.format(info.get('link')))
                            except Exception as error:
                                logger.warning(error)
                            if data:
                                dataList.append(data)
                        if dataList:
                            return dataList
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                logger.warning(error)

        elif info.get('Type') == '国际装置投产及检修计划汇总':
            # print(soup)
            if soup.find('tbody'):
                gs_set = set()
                zz_set = set()
                cn_set = set()
                dq_set = set()
                sj_set = set()
                bz_set = set()

                info = soup.find('tbody').find_all('tr')

                """
                    计划检修 / 国外聚乙烯装置检修计划
                """
                num_list = []
                for num, tr in enumerate(info):
                    try:
                        if len(tr.find_all('td')) == 1 and '检修' in tr.find('td').get_text() or len(
                                tr.find_all('td')) == 1 and '投产' in tr.find('td').get_text():
                            if '2020' in tr.find('td').get_text() or '2021' in tr.find(
                                    'td').get_text() or '2022' in tr.find('td').get_text() or '2023' in tr.find(
                                'td').get_text():
                                num_list.append(num)
                            elif tr.find('td').get_text() == '国外聚乙烯装置投产计划' or tr.find('td').get_text() == '国外聚乙烯装置检修计划':
                                num_list.append(num)
                            else:
                                pass
                    except:
                        pass

                if len(num_list) == 1:
                    infoList = []
                    name = info[num_list[0]].get_text().strip()
                    titles = [td.get_text() for td in info[num_list[0] + 1].find_all('td')]
                    # 捕获完成数据
                    for tr in info[num_list[0] + 2:]:
                        try:
                            if len(tr.find_all('td')) == len(titles):
                                gs_set.add(tr.find_all('td')[0].get_text().strip())
                                zz_set.add(tr.find_all('td')[1].get_text().strip())
                                cn_set.add(tr.find_all('td')[2].get_text().strip())
                                dq_set.add(tr.find_all('td')[3].get_text().strip())
                                sj_set.add(tr.find_all('td')[4].get_text().strip())
                                bz_set.add(tr.find_all('td')[5].get_text().strip())
                        except:
                            pass

                    # 比较 缺失项数据
                    for tr in info[num_list[0] + 2:]:
                        data = {}
                        # 录入完成数据
                        if len(tr.find_all('td')) == len(titles):
                            if tr.find_all('td')[1].get_text().strip() == '总共' or tr.find_all('td')[
                                1].get_text().strip() == '总计':
                                infoList.append(
                                    {tr.find_all('td')[1].get_text().strip(): tr.find_all('td')[2].get_text().strip()})
                            else:
                                gs = tr.find_all('td')[0].get_text().strip()
                                zz = tr.find_all('td')[1].get_text().strip()
                                cn = tr.find_all('td')[2].get_text().strip()
                                dq = tr.find_all('td')[3].get_text().strip()
                                sj = tr.find_all('td')[4].get_text().strip()
                                bz = tr.find_all('td')[5].get_text().strip()
                                infoList.append({
                                    '公司名称': gs,
                                    '装置': zz,
                                    '产能（万吨/年）': cn,
                                    '地区': dq,
                                    '时间': sj,
                                    '备注': bz
                                })
                        # 录入不完整数据
                        elif len(tr.find_all('td')) != len(titles):
                            for td in tr.find_all('td'):
                                try:
                                    if td.get_text().strip() in gs_set:
                                        data.update({'公司名称': td.get_text().strip()})
                                except:
                                    pass

                                try:
                                    if td.get_text().strip() in zz_set:
                                        data.update({'装置': td.get_text().strip()})
                                except:
                                    pass

                                try:
                                    if int(td.get_text().strip()):
                                        data.update({'产能（万吨/年）': td.get_text().strip()})
                                except:
                                    pass

                                try:
                                    if td.get_text().strip() in dq_set:
                                        data.update({'地区': td.get_text().strip()})
                                except:
                                    pass

                                try:
                                    if td.get_text().strip() in sj_set:
                                        data.update({'时间': td.get_text().strip()})
                                except:
                                    pass
                            if data:
                                # if not data.get('公司名称'):
                                #     data.update({'公司名称': gs})
                                # if not data.get('装置'):
                                #     data.update({'装置': zz})
                                # if not data.get('产能（万吨/年）'):
                                #     data.update({'产能（万吨/年）': cn})
                                # if not data.get('地区'):
                                #     data.update({'地区': dq})
                                # if not data.get('时间'):
                                #     data.update({'时间': sj})
                                # if not data.get('备注'):
                                #     data.update({'备注': bz})
                                infoList.append(data)
                    if infoList:
                        dataList.append({
                            'Type': '国外聚乙烯装置检修计划',
                            'dataTitle': name,
                            'dataList': infoList
                        })

                elif len(num_list) == 2:
                    for num in [(num_list[0], num_list[1]), (num_list[1], 0)]:
                        infoList = []
                        name = info[num[0]].get_text().strip()
                        titles = [td.get_text() for td in info[num[0] + 1].find_all('td')]
                        if '检修' in name:
                            Type = '国外聚乙烯装置检修计划'
                        elif '投产' in name:
                            Type = '国外聚乙烯装置投产计划'
                        else:
                            Type = None

                        # 捕获完成数据
                        for tr in info[num_list[0] + 2:]:
                            try:
                                if len(tr.find_all('td')) == len(titles):
                                    gs_set.add(tr.find_all('td')[0].get_text().strip())
                                    zz_set.add(tr.find_all('td')[1].get_text().strip())
                                    cn_set.add(tr.find_all('td')[2].get_text().strip())
                                    dq_set.add(tr.find_all('td')[3].get_text().strip())
                                    sj_set.add(tr.find_all('td')[4].get_text().strip())
                                    bz_set.add(tr.find_all('td')[5].get_text().strip())
                            except:
                                pass

                        # 比较 缺失项数据
                        if num[1] != 0:
                            for tr in info[num[0] + 2: num[1]]:
                                # 录入完整数据
                                if len(tr.find_all('td')) == len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        infoList.append({'总计': tr.find_all('td')[2].get_text().strip()})
                                    else:
                                        gs = tr.find_all('td')[0].get_text().strip()
                                        zz = tr.find_all('td')[1].get_text().strip()
                                        cn = tr.find_all('td')[2].get_text().strip()
                                        dq = tr.find_all('td')[3].get_text().strip()
                                        sj = tr.find_all('td')[4].get_text().strip()
                                        bz = tr.find_all('td')[5].get_text().strip()
                                        infoList.append({
                                            '公司名称': gs,
                                            '装置': zz,
                                            '产能（万吨/年）': cn,
                                            '地区': dq,
                                            '时间': sj,
                                            '备注': bz
                                        })
                                # 录入不完整数据
                                elif len(tr.find_all('td')) != len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        data = {}
                                    else:
                                        data = {}
                                        for td in tr.find_all('td'):
                                            try:
                                                if td.get_text().strip() in gs_set:
                                                    data.update({'公司名称': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in zz_set:
                                                    data.update({'装置': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if int(td.get_text().strip()):
                                                    data.update({'产能（万吨/年）': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in dq_set:
                                                    data.update({'地区': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in sj_set:
                                                    data.update({'时间': td.get_text().strip()})
                                            except:
                                                pass
                                    if data:
                                        # if not data.get('公司名称'):
                                        #     data.update({'公司名称': gs})
                                        # if not data.get('装置'):
                                        #     data.update({'装置': zz})
                                        # if not data.get('产能（万吨/年）'):
                                        #     data.update({'产能（万吨/年）': cn})
                                        # if not data.get('地区'):
                                        #     data.update({'地区': dq})
                                        # if not data.get('时间'):
                                        #     data.update({'时间': sj})
                                        # if not data.get('备注'):
                                        #     data.update({'备注': bz})
                                        infoList.append(data)
                            if infoList:
                                dataList.append({
                                    'Type': Type,
                                    'dataTitle': name,
                                    'dataList': infoList
                                })
                        else:
                            for tr in info[num[0] + 2:]:
                                # 录入完成数据
                                if len(tr.find_all('td')) == len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        infoList.append({'总计': tr.find_all('td')[2].get_text().strip()})
                                    else:
                                        gs = tr.find_all('td')[0].get_text().strip()
                                        zz = tr.find_all('td')[1].get_text().strip()
                                        cn = tr.find_all('td')[2].get_text().strip()
                                        dq = tr.find_all('td')[3].get_text().strip()
                                        sj = tr.find_all('td')[4].get_text().strip()
                                        bz = tr.find_all('td')[5].get_text().strip()
                                        infoList.append({
                                            '公司名称': gs,
                                            '装置': zz,
                                            '产能（万吨/年）': cn,
                                            '地区': dq,
                                            '时间': sj,
                                            '备注': bz
                                        })
                                # 录入不完整数据
                                elif len(tr.find_all('td')) != len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        data = {}
                                    else:
                                        data = {}
                                        for td in tr.find_all('td'):
                                            try:
                                                if td.get_text().strip() in gs_set:
                                                    data.update({'公司名称': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in zz_set:
                                                    data.update({'装置': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if int(td.get_text().strip()):
                                                    data.update({'产能（万吨/年）': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in dq_set:
                                                    data.update({'地区': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in sj_set:
                                                    data.update({'时间': td.get_text().strip()})
                                            except:
                                                pass
                                    if data:
                                        # if not data.get('公司名称'):
                                        #     data.update({'公司名称': gs})
                                        # if not data.get('装置'):
                                        #     data.update({'装置': zz})
                                        # if not data.get('产能（万吨/年）'):
                                        #     data.update({'产能（万吨/年）': cn})
                                        # if not data.get('地区'):
                                        #     data.update({'地区': dq})
                                        # if not data.get('时间'):
                                        #     data.update({'时间': sj})
                                        # if not data.get('备注'):
                                        #     data.update({'备注': bz})
                                        infoList.append(data)

                            if infoList:
                                dataList.append({
                                    'Type': Type,
                                    'dataTitle': name,
                                    'dataList': infoList
                                })

                elif len(num_list) == 3:
                    for num in [(num_list[0], num_list[1]), (num_list[1], num_list[2]), (num_list[2], 0)]:
                        infoList = []
                        name = info[num[0]].get_text().strip()
                        # print(name)
                        titles = [td.get_text() for td in info[num[0] + 1].find_all('td')]
                        # print(titles)
                        if '检修' in name:
                            Type = '计划检修'
                        elif '投产' in name:
                            Type = '国外聚乙烯装置投产计划'
                        else:
                            Type = None

                        # 捕获完成数据
                        for tr in info[num_list[0] + 2:]:
                            try:
                                if len(tr.find_all('td')) == len(titles):
                                    gs_set.add(tr.find_all('td')[0].get_text().strip())
                                    zz_set.add(tr.find_all('td')[1].get_text().strip())
                                    cn_set.add(tr.find_all('td')[2].get_text().strip())
                                    dq_set.add(tr.find_all('td')[3].get_text().strip())
                                    sj_set.add(tr.find_all('td')[4].get_text().strip())
                                    bz_set.add(tr.find_all('td')[5].get_text().strip())
                            except:
                                pass

                        # 比较 缺失项数据
                        if num[1] != 0:
                            for tr in info[num[0] + 2: num[1]]:
                                # 录入完整数据
                                if len(tr.find_all('td')) == len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        infoList.append({'总计': tr.find_all('td')[2].get_text().strip()})
                                    else:
                                        gs = tr.find_all('td')[0].get_text().strip()
                                        zz = tr.find_all('td')[1].get_text().strip()
                                        cn = tr.find_all('td')[2].get_text().strip()
                                        dq = tr.find_all('td')[3].get_text().strip()
                                        sj = tr.find_all('td')[4].get_text().strip()
                                        bz = tr.find_all('td')[5].get_text().strip()
                                        infoList.append({
                                            '公司名称': gs,
                                            '装置': zz,
                                            '产能（万吨/年）': cn,
                                            '地区': dq,
                                            '时间': sj,
                                            '备注': bz
                                        })
                                # 录入不完整数据
                                elif len(tr.find_all('td')) != len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        data = {}
                                    else:
                                        data = {}
                                        for td in tr.find_all('td'):
                                            try:
                                                if td.get_text().strip() in gs_set:
                                                    data.update({'公司名称': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in zz_set:
                                                    data.update({'装置': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if int(td.get_text().strip()):
                                                    data.update({'产能（万吨/年）': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in dq_set:
                                                    data.update({'地区': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in sj_set:
                                                    data.update({'时间': td.get_text().strip()})
                                            except:
                                                pass
                                    if data:
                                        # if not data.get('公司名称'):
                                        #     data.update({'公司名称': gs})
                                        # if not data.get('装置'):
                                        #     data.update({'装置': zz})
                                        # if not data.get('产能（万吨/年）'):
                                        #     data.update({'产能（万吨/年）': cn})
                                        # if not data.get('地区'):
                                        #     data.update({'地区': dq})
                                        # if not data.get('时间'):
                                        #     data.update({'时间': sj})
                                        # if not data.get('备注'):
                                        #     data.update({'备注': bz})
                                        infoList.append(data)
                            if infoList:
                                dataList.append({
                                    'Type': Type,
                                    'dataTitle': name,
                                    'dataList': infoList
                                })
                        else:
                            for tr in info[num[0] + 2:]:
                                # 录入完成数据
                                if len(tr.find_all('td')) == len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        infoList.append({'总计': tr.find_all('td')[2].get_text().strip()})
                                    else:
                                        gs = tr.find_all('td')[0].get_text().strip()
                                        zz = tr.find_all('td')[1].get_text().strip()
                                        cn = tr.find_all('td')[2].get_text().strip()
                                        dq = tr.find_all('td')[3].get_text().strip()
                                        sj = tr.find_all('td')[4].get_text().strip()
                                        bz = tr.find_all('td')[5].get_text().strip()
                                        infoList.append({
                                            '公司名称': gs,
                                            '装置': zz,
                                            '产能（万吨/年）': cn,
                                            '地区': dq,
                                            '时间': sj,
                                            '备注': bz
                                        })
                                # 录入不完整数据
                                elif len(tr.find_all('td')) != len(titles):
                                    if '总计' in [td.get_text().strip() for td in tr.find_all('td')]:
                                        data = {}
                                    else:
                                        data = {}
                                        for td in tr.find_all('td'):
                                            try:
                                                if td.get_text().strip() in gs_set:
                                                    data.update({'公司名称': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in zz_set:
                                                    data.update({'装置': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if int(td.get_text().strip()):
                                                    data.update({'产能（万吨/年）': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in dq_set:
                                                    data.update({'地区': td.get_text().strip()})
                                            except:
                                                pass

                                            try:
                                                if td.get_text().strip() in sj_set:
                                                    data.update({'时间': td.get_text().strip()})
                                            except:
                                                pass
                                    if data:
                                        # if not data.get('公司名称'):
                                        #     data.update({'公司名称': gs})
                                        # if not data.get('装置'):
                                        #     data.update({'装置': zz})
                                        # if not data.get('产能（万吨/年）'):
                                        #     data.update({'产能（万吨/年）': cn})
                                        # if not data.get('地区'):
                                        #     data.update({'地区': dq})
                                        # if not data.get('时间'):
                                        #     data.update({'时间': sj})
                                        # if not data.get('备注'):
                                        #     data.update({'备注': bz})
                                        infoList.append(data)

                            if infoList:
                                dataList.append({
                                    'Type': Type,
                                    'dataTitle': name,
                                    'dataList': infoList
                                })
            if dataList:
                return dataList

        elif info.get('Type') == '聚乙烯企业开工率跟踪报道':
            try:
                text = soup.find('div', {'id': 'content'}).get_text()
                if '。' in text:
                    for content in text.split('。'):
                        if '%' in content:
                            for info in content.split('%'):
                                data_time = re.findall('（(.*?)）', info, re.S)
                                if data_time:
                                    data_time = data_time[0]
                                    year = str(time.strftime("%Y", time.localtime(time.time())))
                                    if '日' in data_time and '月' in data_time:
                                        if '月' in str(data_time).split('-')[0] and '月' not in str(data_time).split('-')[
                                            1]:
                                            month = \
                                                str(data_time).replace('月', '/').replace('日', '').split('-')[0].split(
                                                    '/')[
                                                    0]
                                            data_time = (year + '/' +
                                                         str(data_time).replace('月', '/').replace('日', '').split('-')[
                                                             0]) + '-' + (year + '/{}/'.format(month) +
                                                                          str(data_time).replace('月', '/').replace('日',
                                                                                                                   '').split(
                                                                              '-')[1])
                                        else:
                                            data_time = (year + '/' +
                                                         str(data_time).replace('月', '/').replace('日', '').split('-')[
                                                             0]) + '-' + (year + '/' +
                                                                          str(data_time).replace('月', '/').replace('日',
                                                                                                                   '').split(
                                                                              '-')[1])
                                rate = re.findall('\d+\.\d+', info, re.S)
                                if rate:
                                    rate = rate[0] + '%'

                                dataList.append({data_time: rate})
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == '国内石化PE生产比例汇总':
            try:
                if soup.find('tbody'):
                    # # 纵向
                    # if '日' in soup.find('tbody').find_all('tr')[0].find_all('td')[2].get_text() or '月' in soup.find('tbody').find_all('tr')[0].find_all('td')[2].get_text():
                    #     print('纵向--长')
                    #     if len(soup.find('tbody').find_all('tr')) == 15:
                    #         titles = [td.get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip() for td in soup.find('tbody').find_all('tr')[0].find_all('td')]
                    #         values = soup.find('tbody').find_all('tr')
                    #         for num in range(1, len(soup.find('tbody').find_all('tr')[0].find_all('td')[1:]) + 1):
                    #             try:
                    #                 dataList.append({
                    #                     '日期': titles[num],
                    #                     '高压': {
                    #                         '高压': values[1].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                    '').replace('\t',
                    #                                                                                                '').replace(
                    #                             '\r', '').strip(),
                    #                         '涂覆': values[2].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                    '').replace('\t',
                    #                                                                                                '').replace(
                    #                             '\r', '').strip()
                    #                     },
                    #                     '低压管材': {
                    #                         '80级管材': values[3].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                       '').replace(
                    #                             '\t', '').replace('\r', '').strip(),
                    #                         '100级管材': values[4].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                        '').replace(
                    #                             '\t', '').replace('\r', '').strip(),
                    #                         '地暖级管材': values[5].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                       '').replace(
                    #                             '\t', '').replace('\r', '').strip()
                    #                     },
                    #                     '低压中空': {
                    #                         '大中空': values[6].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                     '').replace(
                    #                             '\t', '').replace('\r', '').strip(),
                    #                         '小中空': values[7].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                     '').replace(
                    #                             '\t', '').replace('\r', '').strip()
                    #                     },
                    #                     '低压注塑': {
                    #                         '低熔注塑': values[8].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                      '').replace(
                    #                             '\t', '').replace('\r', '').strip(),
                    #                         '高熔注塑': values[9].find_all('td')[num + 1].get_text().replace('\n',
                    #                                                                                      '').replace(
                    #                             '\t', '').replace('\r', '').strip()
                    #                     },
                    #                     '低压薄膜': values[10].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                    #                         '\t', '').replace('\r', '').strip(),
                    #                     '低压拉丝': values[11].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                    #                         '\t', '').replace('\r', '').strip(),
                    #                     '线性': values[12].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                    #                         '\t', '').replace('\r', '').strip(),
                    #                     '其他': values[13].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                    #                         '\t', '').replace('\r', '').strip(),
                    #                     '检修': values[14].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                    #                         '\t', '').replace('\r', '').strip()
                    #                 })
                    #             except:
                    #                 pass
                    # # 横向
                    # else:
                    #     if len(soup.find('tbody').find_all('tr')[1].find_all('td')) == 10:
                    #         print('横向--短')
                    #         for tr in soup.find('tbody').find_all('tr')[1:]:
                    #             data = {}
                    #             titles = [td.get_text().replace('\n', '').replace('\r', '').replace('\t', '').strip() for td in soup.find('tbody').find_all('tr')[0].find_all('td')]
                    #             if '日期' not in tr.find_all('td')[0].get_text():
                    #                 try:
                    #                     for num in range(len(titles)):
                    #                         value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                    #                                                                                             '').replace(
                    #                             '\r', '').strip()
                    #                         data.update({
                    #                             titles[num]: value
                    #                         })
                    #                 except:
                    #                     pass
                    #             if data:
                    #                 dataList.append(data)
                    #
                    #     elif len(soup.find('tbody').find_all('tr')[1].find_all('td')) != 10:
                    #         try:
                    #             if len(soup.find('tbody').find_all('tr')[2].find_all('td')) == 15:
                    #                 print('横向--长')
                    #                 for tr in soup.find('tbody').find_all('tr')[2:]:
                    #                     data = {}
                    #                     titles = ['日期', '高压', '涂覆', '80级管材', '100级管材', '地暖级管材', '大中空', '小中空',
                    #                               '低熔注塑', '高熔注塑', '低压薄膜', '低压拉丝', '线性', '其他', '检修']
                    #                     if '日期' not in tr.find_all('td')[0].get_text():
                    #                         try:
                    #                             for num in range(len(titles)):
                    #                                 value = tr.find_all('td')[num].get_text().replace('\n', '').replace(
                    #                                     '\t', '').replace('\r', '').strip()
                    #                                 if '日' in value or '月' in value:
                    #                                     value = info.get('uploadTime').split(' ')[0].replace('[',
                    #                                                                                          '').replace(
                    #                                         '-', '/')
                    #                                 data.update({
                    #                                     titles[num]: value
                    #                                 })
                    #                         except:
                    #                             pass
                    #                     if data:
                    #                         dataList.append(data)
                    #         except:
                    #             pass
                    #         try:
                    #             if len(soup.find('table', {'align': 'center'}).find_all('tr')[1].find_all('td')) == 15:
                    #                 print('横向--长')
                    #                 for tr in soup.find('table', {'align': 'center'}).find_all('tr')[1:]:
                    #                     data = {}
                    #                     titles = ['日期', '高压', '涂覆', '80级管材', '100级管材', '地暖级管材', '大中空', '小中空',
                    #                               '低熔注塑', '高熔注塑', '低压薄膜', '低压拉丝', '线性', '其他', '检修']
                    #                     if '日期' not in tr.find_all('td')[0].get_text():
                    #                         try:
                    #                             for num in range(len(titles)):
                    #                                 value = tr.find_all('td')[num].get_text().replace('\n', '').replace(
                    #                                     '\t', '').replace('\r', '').strip()
                    #                                 if '日' in value or '月' in value:
                    #                                     value = info.get('uploadTime').split(' ')[0].replace('[',
                    #                                                                                          '').replace(
                    #                                         '-', '/')
                    #                                 data.update({
                    #                                     titles[num]: value
                    #                                 })
                    #                         except:
                    #                             pass
                    #                     if data:
                    #                         dataList.append(data)
                    #         except:
                    #             pass
                    #
                    #     else:
                    #         print('出现新界面！')
                    if len(soup.find('tbody').find_all('tr')[0].find_all('td')) == 10 and len(soup.find('tbody').find_all('tr')[1].find_all('td')) == 10 and len(soup.find('tbody').find_all('tr')[2].find_all('td')) == 16:
                        for tr in soup.find('tbody').find_all('tr')[2:]:
                            tds = tr.find_all('td')
                            try:
                                dataList.append({
                                    '日期': tds[0].get_text().replace('\n','').replace('\t','').replace('\r', '').strip(),
                                    '高压': {
                                        '高压': tds[1].get_text().replace('\n','').replace('\t','').replace('\r', '').strip(),
                                        '涂覆': tds[2].get_text().replace('\n','').replace('\t','').replace('\r', '').strip()
                                    },
                                    '低压管材': {
                                        '80级管材': tds[3].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '100级管材': tds[4].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '地暖级管材': tds[5].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '其他管材': tds[6].get_text().replace('\n', '').replace('\t', '').replace('\r','').strip()
                                    },
                                    '低压中空': {
                                        '大中空': tds[7].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '小中空': tds[8].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip()
                                    },
                                    '低压注塑': {
                                        '低熔注塑': tds[9].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '高熔注塑': tds[10].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip()
                                    },
                                    '低压薄膜': tds[11].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '拉丝': tds[12].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '线性': tds[13].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '其他': tds[14].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '检修': tds[15].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()
                                })
                            except:
                                pass
                    elif len(soup.find('tbody').find_all('tr')[0].find_all('td')) == 10 and len(soup.find('tbody').find_all('tr')[1].find_all('td')) == 9 and len(soup.find('tbody').find_all('tr')[2].find_all('td')) == 15:
                        for tr in soup.find('tbody').find_all('tr')[2:]:
                            tds = tr.find_all('td')
                            try:
                                dataList.append({
                                    '日期': tds[0].get_text().replace('\n','').replace('\t','').replace('\r', '').strip(),
                                    '高压': {
                                        '高压': tds[1].get_text().replace('\n','').replace('\t','').replace('\r', '').strip(),
                                        '涂覆': tds[2].get_text().replace('\n','').replace('\t','').replace('\r', '').strip()
                                    },
                                    '低压管材': {
                                        '80级管材': tds[3].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '100级管材': tds[4].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '地暖级管材': tds[5].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip()
                                    },
                                    '低压中空': {
                                        '大中空': tds[6].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '小中空': tds[7].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip()
                                    },
                                    '低压注塑': {
                                        '低熔注塑': tds[8].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip(),
                                        '高熔注塑': tds[9].get_text().replace('\n','').replace('\t', '').replace('\r', '').strip()
                                    },
                                    '低压薄膜': tds[10].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '拉丝': tds[11].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '线性': tds[12].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '其他': tds[13].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip(),
                                    '检修': tds[14].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()
                                })
                            except:
                                pass
                    else:
                        self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                        print('出现新的页面结构！')
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == '进出口数据':
            try:
                if '贸易方式' in soup.find('div', {'class': 'bord_box'}).find_all('tr')[0].get_text().strip():
                    for tr in soup.find('div', {'class': 'bord_box'}).find_all('tr')[1:]:
                        dataList.append({
                            '贸易方式': tr.find_all('td')[0].get_text().strip(),
                            '数量': tr.find_all('td')[1].get_text().strip()
                        })
                elif '产销国' in soup.find('div', {'class': 'bord_box'}).find_all('tr')[0].get_text().strip():
                    for tr in soup.find('div', {'class': 'bord_box'}).find_all('tr')[1:]:
                        dataList.append({
                            '产销国': tr.find_all('td')[0].get_text().strip(),
                            '数量': tr.find_all('td')[1].get_text().strip()
                        })
                else:
                    if '贸易伙伴' in soup.find('div', {'class': 'bord_box'}).find_all('tr')[0].get_text().strip():
                        self.message_coll.delete_one({'link': link})
                        print('贸易伙伴')
                    elif '国家' in soup.find('div', {'class': 'bord_box'}).find_all('tr')[0].get_text().strip():
                        self.message_coll.update_one({'link': link}, {'$set': {'status': '国家'}}, upsert=True)
                        print('国家')
                    else:
                        print('表格有变化！')
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == '生产企业库存早报':
            try:
                date = \
                    soup.find('div', {'class': 'div1 xqbox'}).find('span').get_text().split(' ')[0].replace('-',
                                                                                                            '/').split(
                        '：')[1]
                info = soup.find('div', {'id': 'content'}).get_text().split('，')
                # print(info)
                for msg in info:
                    try:
                        value = re.findall('两油库存(\d+\.\d+|\d+|\d)', msg, re.S)
                        if value:
                            value = value
                        else:
                            value = re.findall('今日库存(\d+\.\d+|\d+|\d)', msg, re.S)
                            if value:
                                value = value
                            else:
                                value = re.findall('两油库(\d+\.\d+|\d+|\d)', msg, re.S)

                        if value:
                            dataList.append({
                                date: value[0]
                            })
                        try:
                            fontValue = re.findall('库存修正至(\d+\.\d+|\d+|\d)', msg, re.S)
                            if fontValue:
                                fontValue = fontValue[0]
                            else:
                                fontValue = re.findall('修正库存(\d+\.\d+|\d+|\d)', msg, re.S)
                                if fontValue:
                                    fontValue = fontValue[0]
                                else:
                                    fontValue = re.findall('修正早库(\d+\.\d+|\d+|\d)', msg, re.S)
                                    if fontValue:
                                        fontValue = fontValue[0]
                            if fontValue:
                                ntime = \
                                    soup.find('div', {'class': 'xq-head'}).find('span').get_text().split(' ')[0].split(
                                        '：')[
                                        1]
                                fontTime = time.strftime("%Y-%m-%d", time.localtime(
                                    int(time.mktime(time.strptime(ntime, "%Y-%m-%d"))) - 86400))
                                if fontTime and fontValue:
                                    dataList.append({
                                        fontTime.replace('-', '/'): fontValue
                                    })
                        except:
                            pass
                    except:
                        pass
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == '主要港口库存动态':
            try:
                date = soup.find('div', {'id': 'content'}).get_text()
                data_value = re.findall('截止(.*?)日，', date, re.S)
                if data_value:
                    data_value = data_value[0].replace('年', '/').replace('月', '/').replace('日', '/')
                info = soup.find('div', {'id': 'content'}).get_text()
                info_value = re.findall('港口样本库存总量在(.*?)万吨', info, re.S)
                if info_value:
                    info_value = info_value[0]
                dataList.append({
                    data_value: info_value
                })
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == 'PE包装膜企业开工率':
            try:
                date = \
                    soup.find('div', {'class': 'div1 xqbox'}).find('span').get_text().split(' ')[0].replace('-',
                                                                                                            '/').split(
                        '：')[1]

                for p in soup.find('div', {'class': 'xq-content'}).find_all('p'):
                    try:
                        info = p.get_text()
                        value = re.findall('开工率(\d+|\d+\.\d+)%', info, re.S)
                        if value:
                            value = value[0]
                        else:
                            value = 0
                        rate = re.findall('周环比(.*?)%', info, re.S)
                        if rate:
                            rate = rate[0]
                        else:
                            rate = re.findall('开工率(.*?)%', info.replace('较上周', '').replace('下降', '-'), re.S)
                            if rate:
                                rate = rate[0]
                        if value or rate:
                            dataList.append({
                                'date': date,
                                'value': value,
                                'rate': rate
                            })
                    except:
                        pass
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == '农膜日评':
            try:
                date = re.findall('\d+-\d+-\d+', soup.find('div', {'style': 'float: left'}).get_text(), re.S)[0]

                if '类别' in soup.find('tbody').find_all('tr')[0].find_all('td')[0].get_text().strip() and '山东' in \
                        soup.find('tbody').find_all('tr')[0].find_all('td')[1].get_text().strip():
                    data = {}
                    for tr in soup.find('tbody').find_all('tr')[1:]:
                        data.update({
                            tr.find_all('td')[0].get_text().strip(): tr.find_all('td')[1].get_text().strip()
                        })
                else:
                    data = None

                if date and data:
                    dataList.append({date: data})
            except Exception as error:
                logger.warning(error)

            return dataList

        elif info.get('Type') == '塑膜收盘价格表':
            date = re.findall('\d+-\d+-\d+', soup.find('div', {'style': 'float: left'}).get_text(), re.S)[0]
            try:
                if 'Panel_Login' in str(Html) or '未找到您的权限信息' in str(Html):
                    print('未找到您的权限信息，请重新登录')
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无权限'}}, upsert=True)
                    return

                if soup.find('tbody'):
                    if '产品' in soup.find('tbody').find_all('tr')[0].find_all('td')[0].get_text().strip() \
                            and '规格' in soup.find('tbody').find_all('tr')[0].find_all('td')[1].get_text().strip() \
                            and ('日' in soup.find('tbody').find_all('tr')[0].find_all('td')[2].get_text().strip()
                                 or '-' in soup.find('tbody').find_all('tr')[0].find_all('td')[2].get_text().strip()):
                        data = {}
                        for tr in soup.find('tbody').find_all('tr')[1:]:
                            if tr.find_all('td')[0].get_text().strip() == 'PE缠绕膜':
                                data.update({
                                    tr.find_all('td')[1].get_text().strip(): tr.find_all('td')[2].get_text().strip()
                                })
                        if date and data:
                            dataList.append({date: data})
                    elif '企业名称' in soup.find('tbody').find_all('tr')[0].get_text().strip():
                        self.message_coll.update_one({'link': link}, {'$set': {'status': '企业名称'}}, upsert=True)
            except Exception as error:
                logger.warning(error)
            return dataList

        elif info.get('Type') == '神华PE竞拍':
            try:
                if soup.find('tbody'):
                    titles = [td.get_text().strip() for td in soup.find('tbody').find_all('tr')[0].find_all('td')]
                    for tr in soup.find('tbody').find_all('tr')[1:]:
                        data = {}
                        try:
                            if len(tr.find_all('td')) == len(titles):
                                self.titleOne = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                    '\r', '').strip()
                                self.titleTwo = tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                    '\r', '').strip()
                                self.titleThree = tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                    '\r', '').strip()

                                for num in range(len(titles)):
                                    key = titles[num]
                                    value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                        '\r', '').strip()
                                    data.update({
                                        key: value
                                    })
                            elif len(tr.find_all('td')) + 1 == len(titles):
                                self.titleTwo = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                    '\r', '').strip()
                                self.titleThree = tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                    '\r', '').strip()
                                data.update({
                                    titles[0]: self.titleOne
                                })

                                for num in range(len(tr.find_all('td'))):
                                    key = titles[num + 1]
                                    value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                        '\r', '').strip()
                                    data.update({
                                        key: value
                                    })
                            elif len(tr.find_all('td')) + 2 == len(titles):
                                self.titleThree = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                    '\r', '').strip()
                                data.update({
                                    titles[0]: self.titleOne,
                                    titles[1]: self.titleTwo
                                })

                                for num in range(len(tr.find_all('td'))):
                                    key = titles[num + 2]
                                    value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                        '\r', '').strip()
                                    data.update({
                                        key: value
                                    })
                            elif '竞拍总量' in str(tr) or '成交总量' in str(tr):
                                data.update({
                                    self.titleOne + tr.find_all('td')[0].get_text().strip()[:4]: tr.find_all('td')[
                                        1].get_text().strip(),
                                    self.titleOne + tr.find_all('td')[0].get_text().strip()[:4] + '成交率':
                                        tr.find_all('td')[-1].get_text().strip()
                                })
                            else:
                                logger.warning('表头对不上！  {}'.format(info.get('link')))
                        except:
                            pass
                        if data:
                            dataList.append(data)
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                logger.warning(error)
            return dataList

        elif info.get('Type') == 'PE国内企业装置检修':
            try:
                if soup.find('tbody'):
                    # 表头
                    titles = [td.get_text().strip() for td in soup.find('tbody').find_all('tr')[0].find_all('td')]

                    for tr in soup.find('tbody').find_all('tr')[1:]:
                        data = {}
                        try:
                            for num in range(len(titles)):
                                data.update({
                                    titles[num]: tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                             '').replace(
                                        '\r', '').strip()
                                })
                        except:
                            pass
                        if data:
                            dataList.append(data)
                    if dataList:
                        self.articleData_coll.update_one({'link': link}, {'$set': {
                            'link': link,
                            'Type': info['Type'],
                            'title': info['title'],
                            'uploadTime': info.get('publishTime'),
                            'dataList': dataList
                        }}, upsert=True)

                        self.message_coll.update_one({'link': link}, {'$set': {'status': 1}}, upsert=True)
                        print(dataList)

                        return 1
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                logger.warning(error)

        elif info.get('Type') == '国内PP装置检修':
            try:
                if soup.find_all('tbody'):
                    for tbody_num in range(len(soup.find_all('tbody'))):
                        tbody = soup.find_all('tbody')[tbody_num]

                        # 标题
                        tb_title = None
                        for p in soup.find('div', {'class': 'xq-content'}).find_all('p'):
                            try:
                                if tbody_num == 0:
                                    if '表1 ' in str(p):
                                        tb_title = p.get_text()
                                        break
                                    else:
                                        tb_title = None
                            except:
                                tb_title = None

                            try:
                                if tbody_num == 1:
                                    if '表2' in str(p):
                                        tb_title = p.get_text()
                                        break
                                    else:
                                        tb_title = None
                            except:
                                tb_title = None

                        # 表头
                        titles = [td.get_text().strip() for td in tbody.find_all('tr')[0].find_all('td')]

                        # 表格数据
                        tb_data_list = []
                        for tr in tbody.find_all('tr')[1:]:
                            try:
                                tb_data = {}
                                for num in range(len(titles)):
                                    tb_data.update({
                                        titles[num]: tr.find_all('td')[num].get_text().replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                            '\r', '').replace('\xa0', '').strip()
                                    })
                                if tb_data:
                                    tb_data_list.append(tb_data)
                            except:
                                pass

                        if tb_data_list:
                            dataList.append({
                                'tb_title': tb_title,
                                'tb_data': tb_data_list
                            })
                    if dataList:
                        self.pp_articleData_coll.update_one({'link': link}, {'$set': {
                            'link': link,
                            'Type': info['Type'],
                            'title': info['title'],
                            'uploadTime': info.get('publishTime'),
                            'dataList': dataList
                        }}, upsert=True)

                        self.message_coll.update_one({'link': link}, {'$set': {'status': 1}}, upsert=True)
                        print(dataList)

                        return 1
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                logger.warning(error)

        else:
            print('未知类型 - {}'.format(info.get('Type')))

    """
        PE/PP国内企业装置检修
    """

    def GetPeLongzhong(self, info, proxy=False, history=False, pageNum=1):
        print('第{}页'.format(pageNum))
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '120',
            'Content-Type': 'application/json;charset=UTF-8',
            'Host': 'search.oilchem.net',
            'Origin': 'https://search.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://search.oilchem.net/article.html?keyword=PE%E5%9B%BD%E5%86%85%E4%BC%81%E4%B8%9A%E8%A3%85%E7%BD%AE%E6%A3%80%E4%BF%AE',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        jsonData = {
            "query": info.get('Type'),
            "pageNo": pageNum,
            "pageSize": 10,
            "channelId": "",
            "startPublishTime": "",
            "endPublishTime": ""
        }
        headers.update({
            'Cookie': self.cookie_coll.find_one({'name': 'lz_sj_category'}).get('cookie')
        })

        try:
            resp = requests.post(url=info.get('url'), headers=headers, json=jsonData, timeout=5, verify=False)
            resp.encoding = 'utf-8'

            if resp.status_code == 200:
                dataList = self.ParsePeLongzhong(resp.json())
                if dataList:
                    for msg in dataList:
                        if info.get('Type') in msg['title']:
                            if str(msg['url']).startswith('//news.oilchem.net'):
                                msg['url'] = 'https' + msg['url']
                            msg.update({'Type': info.get('Type')})
                            self.message_coll.update_one({'link': msg['url']}, {'$set': msg}, upsert=True)
                            print(msg)

                    if history:
                        return self.GetPeLongzhong(info, proxy, history, pageNum + 1)
                else:
                    print('没有数据！')
        except requests.exceptions.ConnectionError:
            return self.GetPeLongzhong(info, proxy, history, pageNum)
        except Exception as error:
            logger.warning(error)

    @staticmethod
    def ParsePeLongzhong(jsonHtml):
        if jsonHtml.get('dataList'):
            for info in jsonHtml.get('dataList'):
                try:
                    info['publishTime'] = time.strftime("%Y-%m-%d %H:%M",
                                                        time.localtime(int(info.get('publishTime') / 1000)))
                    info['title'] = info.get('title') + info.get('publishTime')
                except Exception as error:
                    logger.warning(error)
            return jsonHtml.get('dataList')
        else:
            return None

    # 还原状态
    @staticmethod
    def removeStatus(coll, key):
        for info in coll.find({'$nor': [{'status': 401}, {'status': 404}]}):
            coll.update_one({key: info[key]}, {'$unset': {'status': ''}}, upsert=True)

    """
        多进程执行
    """

    # 多进程获取数据
    def CommandThread(self, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=3)

        for info in self.message_coll.find({'$nor': [{'status': 1}]}).sort('_id', -1):
        # for info in self.message_coll.find({"Type" : "国内石化PE生产比例汇总"}).sort('_id', -1):
            if Async:
                out = pool.apply_async(func=self.GetUrlFromMongo, args=(info,))  # 异步
            else:
                out = pool.apply(func=self.GetUrlFromMongo, args=(info,))  # 同步
            thread_list.append(out)
            # break

        pool.close()
        pool.join()


def perun():
    pe = PE()

    # 1 初始化翻第一页  True 加载历史数据  False 不加载历史数据
    for info in [
        {'url': 'https://plas.chem99.com/news/?page={page}&sid=9978&siteid=3', 'Type': '国内PE装置动态汇总'},
        {'url': 'https://plas.chem99.com/news/?page={page}&sid=4520&siteid=3', 'Type': '农膜日评'},
        {'url': 'https://plas.chem99.com/news/?page={page}&sid=4602&siteid=3', 'Type': '塑膜收盘价格表'},
        {'url': 'https://plas.chem99.com/news/?page={page}&sid=610&siteid=3', 'Type': '神华PE竞拍'},
        {'url': 'https://plas.chem99.com/news/?page={page}&sid=9978&k=1&sname=%e4%b8%ad%e7%9f%b3%e5%8c%96&siteid=3',
         'Type': '国内石化PE生产比例汇总'},

        {'url': 'https://list.oilchem.net/328/3951/{page}.html', 'Type': '国际装置投产及检修计划汇总'},
        {'url': 'https://list.oilchem.net/328/3968/{page}.html', 'Type': '聚乙烯企业开工率跟踪报道'},
        {'url': 'https://list.oilchem.net/328/550/{page}.html', 'Type': '生产企业库存早报'},
        {'url': 'https://list.oilchem.net/328/550/{page}.html', 'Type': '主要港口库存动态'},
        {'url': 'https://list.oilchem.net/3728/29892/{page}.html', 'Type': 'PE包装膜企业开工率'},

        {
            'url': 'http://plas.315i.com/common/goArticleList?pageIndex={page}&productIds=004001001&columnIds=005010&type=0&pageId=255',
            'Type': '进出口数据'},
    ]:
        # pass
        # print(info['Type'])
        pe.GetAllMessages(info)

    for info in [
        {'url': 'https://search.oilchem.net/oilsearch/newsearch/oilchem/search/searchArticle', 'Type': 'PE国内企业装置检修'},
        {'url': 'https://search.oilchem.net/oilsearch/newsearch/oilchem/search/searchArticle', 'Type': '国内PP装置检修'},
    ]:
        # pass
        # print(info['Type'])
        pe.GetPeLongzhong(info)

    # 多进程获取数据
    pe.CommandThread()

    # 获取聚丙烯数据
    pe.GetSelectDate()

    print('pe-wangye 获取历史数据--完成')


if __name__ == '__main__':
    perun()
