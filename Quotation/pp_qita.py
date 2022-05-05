#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import sys

sys.path.append("../")
import configparser
import logging
import os
import pprint
import random
import re
import threading
import time
from multiprocessing.pool import ThreadPool
from os import path
from urllib.parse import urlencode

import pymysql
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient

from Cookies.proxy import HandleProxy

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)
logPath = os.path.join(dh + r'/Logs/pp_qita.log')
settingPath = os.path.join(dh + r'/Settings.ini')

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


class PPQita:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")
        proxydb = conf.get("Mongo", "PROXY")

        # client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))

        # cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        # proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        proxyclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]

        self.message_coll = client[datadb]['pp_qita_messages']
        self.articleData_coll = client[datadb]['pp_qita_articleData']

        self.pageApiUrl = 'https://search.oilchem.net/oilsearch/newsearch/oilchem/search/searchArticle'
        self.pageApiHeaders = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '124',
            'Content-Type': 'application/json;charset=UTF-8',
            'Cookie': '_member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkMWM5Lmlnby5GVjZRdFVPaVpsS1hJZVAzU3U2N0VLWC93eFlSTGczaHJNeHlRM0oueGlUT0ciLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMzAxMDMzMywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MTA0MTgzMzMsImp0aSI6ImIyOTRjYzU5LWNkY2MtNGNkNS1hYWMzLTdjMmRkMmEzMDZlMiIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.zTfS8Iqye97xhWqEX0sIMYkK4iZ9x6qvin9np154QJM; _username=13428976742; oilchem_refer_url=; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1611244773,1611316327,1611329826,1611389148; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1611244773,1611316327,1611329826,1611389148; oilchem_land_url=https://news.oilchem.net/20/0403/14/48678e202f56c7d9.html; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1611497120; refcheck=ok; refpay=0; refsite=; allHistory=%5B%22%E5%A1%91%E7%BC%96%E4%BC%81%E4%B8%9A%E5%8E%9F%E6%96%99%E5%BA%93%E5%AD%98%E5%88%86%E6%9E%90%22%2C%22BOPP%E6%88%90%E5%93%81%E5%BA%93%E5%AD%98%E5%88%86%E6%9E%90%22%5D; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1611644155',
            'Host': 'search.oilchem.net',
            'Origin': 'https://search.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://search.oilchem.net/article.html?keyword=%E5%A1%91%E7%BC%96%E4%BC%81%E4%B8%9A%E5%8E%9F%E6%96%99%E5%BA%93%E5%AD%98%E5%88%86%E6%9E%90',
            'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkMWM5Lmlnby5GVjZRdFVPaVpsS1hJZVAzU3U2N0VLWC93eFlSTGczaHJNeHlRM0oueGlUT0ciLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMzAxMDMzMywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MTA0MTgzMzMsImp0aSI6ImIyOTRjYzU5LWNkY2MtNGNkNS1hYWMzLTdjMmRkMmEzMDZlMiIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.zTfS8Iqye97xhWqEX0sIMYkK4iZ9x6qvin9np154QJM',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.articleHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'news.oilchem.net',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.juBingXiApi = 'http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html'
        self.juBingXiHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '54',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'JSESSIONID=FF3708D22745EEECAAD43A510F03FCBE; WMONID=NMEOpDXg0Qc; Hm_lvt_a50228174de2a93aee654389576b60fb=1604997991,1606236323; Hm_lpvt_a50228174de2a93aee654389576b60fb=1606237048',
            'Host': 'www.dce.com.cn',
            'Origin': 'http://www.dce.com.cn',
            'Pragma': 'no-cache',
            'Referer': 'http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.userAgent = UserAgent().random

    @staticmethod
    def MySql():
        # 实例化 Mysql
        host = conf.get("Mysql", "HOST")
        mysqldb = conf.get("Mysql", "MYSQLDB")
        user = conf.get("Mysql", "USER")
        pwd = conf.get("Mysql", "PASSWORD")
        conn = pymysql.Connect(
            host=host,
            port=3306,
            user=user,
            password=pwd,
            db=mysqldb,
            charset='utf8mb4',
            autocommit=True
        )
        return conn

    def GetProxy(self):
        try:
            if self.pros:
                usePro = self.pros.pop()
            else:
                # 计算代理池总数
                if self.proxy_coll.find({'status': 1}).count() < 10:
                    HandleProxy().InsertProxy(1)
                else:
                    self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]
                return self.GetProxy()

            if usePro:
                return {
                    'http': 'http://{}'.format(usePro),
                    'https': 'http://{}'.format(usePro),
                }
            else:
                return
        except:
            return

    def DisProxy(self, pro):
        if isinstance(pro, dict):
            pro = pro.get('http').split('//')[1]

        # 改写数据库IP
        try:
            self.proxy_coll.update_one({'pro': pro}, {'$set': {
                'status': 0,
                'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
            }}, upsert=True)
        except:
            pass

    # 获取最新文章
    def GetAllMessages(self, info, proxy=False, history=False, pageNum=1):
        print('第 {} 页'.format(pageNum))
        try:
            jsonData = {
                'channelId': "",
                'endPublishTime': "",
                'pageNo': pageNum,
                'pageSize': 10,
                'query': info['keyword'],
                'startPublishTime': ""
            }

            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url=self.pageApiUrl, headers=self.pageApiHeaders, proxies=pro, json=jsonData,
                                         verify=False)
                else:
                    resp = requests.post(url=self.pageApiUrl, headers=self.pageApiHeaders, json=jsonData, verify=False)
            else:
                resp = requests.post(url=self.pageApiUrl, headers=self.pageApiHeaders, json=jsonData, verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data = self.ParseMessages(resp.json())
                if data.get('dataList'):
                    if history:
                        for msg in data.get('dataList'):
                            if str(msg['url']).startswith('//news.oilchem.net'):
                                msg['url'] = 'https:' + msg['url']

                            msg['Type'] = info['Type']

                            msg['publishTime'] = time.strftime("%Y-%m-%d %H:%M:%S",
                                                               time.localtime(int(str(msg['publishTime'])[:10])))

                            try:
                                self.message_coll.update_one({'url': msg['url']}, {'$set': msg}, upsert=True)
                            except Exception as error:
                                logger.warning(error)

                        if pageNum <= data.get('maxPage'):
                            time.sleep(3)
                            return self.GetAllMessages(info, proxy, history, pageNum + 1)
                        else:
                            pass
                    else:
                        for msg in data.get('dataList'):
                            msg['publishTime'] = time.strftime("%Y-%m-%d %H:%M:%S",
                                                               time.localtime(int(str(msg['publishTime'])[:10])))

                            if str(time.strftime("%Y-%m-%d", time.localtime(time.time()))) not in str(
                                    msg['publishTime']):
                                try:
                                    msg.update({
                                        'Type': info['Type']
                                    })

                                    try:
                                        self.message_coll.update_one({'url': msg['url']}, {'$set': msg}, upsert=True)
                                    except Exception as error:
                                        logger.warning(error)

                                except Exception as error:
                                    logger.warning(error)
                            else:
                                break
                else:
                    print('没有数据， 检查cookie')
            else:
                print(resp.status_code)
        except requests.exceptions.ConnectionError:
            return self.GetAllMessages(info, proxy, history, pageNum + 1)
        except Exception as error:
            logger.warning(error)

    @staticmethod
    def ParseMessages(jsonData):
        data = {}
        dataList = []
        try:
            if isinstance(jsonData, dict):
                if jsonData.get('dataList') and isinstance(jsonData.get('dataList'), list):
                    dataList = jsonData.get('dataList')
        except Exception as error:
            logger.warning(error)

        try:
            if jsonData.get('total'):
                if int(jsonData.get('total')) % 10 != 0:
                    maxPage = int(int(jsonData.get('total')) / 10) + 1
                else:
                    maxPage = int(int(jsonData.get('total')) / 10)
            else:
                maxPage = None
        except Exception as error:
            logger.warning(error)
            maxPage = None

        if dataList:
            data.update({
                'dataList': dataList,
                'maxPage': maxPage
            })

        return data

    # 获取文章内容
    def GetUrlFromMongo(self, info, proxy=False):
        url = info['url']
        print(url)

        self.articleHeaders.update({'cookie': self.cookie_coll.find_one({'name': 'lz_pp_qita'}).get('cookie')})

        try:
            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(url=url, headers=self.articleHeaders, proxies=pro, verify=False)
                else:
                    resp = requests.get(url=url, headers=self.articleHeaders, verify=False)
            else:
                resp = requests.get(url=url, headers=self.articleHeaders, verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data = self.ParseArticle(info['Type'], resp.text)
                if data:
                    print(data)
                    data.update({
                        "url": info["url"],
                        "Type": info["Type"],
                        "columnName": info["columnName"],
                        "content": info["content"],
                        "esScore": info["esScore"],
                        "navigation": info["navigation"],
                        "title": info["title"],
                        "publishTime": info["publishTime"]
                    })
                    try:
                        self.articleData_coll.update_one({'url': url}, {'$set': data}, upsert=True)
                        self.message_coll.update_one({'url': url}, {'$set': {'status': 1}}, upsert=True)
                    except Exception as error:
                        logger.warning(error)
        except requests.exceptions.ConnectionError:
            # 标记失效代理
            if pro:
                threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetUrlFromMongo(info, True)
        except TimeoutError:
            # 标记失效代理
            if pro:
                threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetUrlFromMongo(info, True)
        except Exception as error:
            logger.warning(error)
            return

        time.sleep(random.uniform(15, 25))

    @staticmethod
    def ParseArticle(Type, Html):
        soup = BeautifulSoup(Html, 'lxml')
        data = {}

        if Type == "塑编企业原料库存分析":
            try:
                # 地区信息
                if soup.find('div', {'class': 'xq-content'}):
                    for p in soup.find('div', {'class': 'xq-content'}).find_all('p'):
                        try:
                            data.update({
                                '区域详情': soup.find('div', {'class': 'xq-content'}).get_text().replace('\n', '').replace(
                                    '\t', '').replace('\r', '').strip()
                            })
                            if '华北地区' in p.get_text():
                                location = '华北地区'
                            elif '华东地区' in p.get_text():
                                location = '华东地区'
                            elif '华中地区' in p.get_text():
                                location = '华中地区'
                            elif p.find('img'):
                                data.update({
                                    'imageLink': 'https:' + p.find('img').get('src')
                                })
                            else:
                                location = None

                            if location and '样本企业原料' in str(p.get_text().strip()):
                                # 周期
                                try:
                                    cycle = re.findall('周期为(.*?)，', str(p.get_text().strip()), re.S)
                                    if cycle:
                                        cycle = cycle[0].replace('天', '')
                                    else:
                                        cycle = re.findall('周期为(.*?)；', str(p.get_text().strip()), re.S)
                                        if cycle:
                                            cycle = cycle[0].replace('天', '')
                                        else:
                                            cycle = re.findall('周期为(.*?)。', str(p.get_text().strip()), re.S)
                                            if cycle:
                                                cycle = cycle[0].replace('天', '')
                                            else:
                                                cycle = re.findall('周期为(.*?)天', str(p.get_text().strip()), re.S)
                                                if cycle:
                                                    cycle = cycle[0].replace('天', '')
                                                else:
                                                    cycle = None
                                except:
                                    cycle = None

                                # 浮动率
                                try:
                                    floating = re.findall('...\d+\.\d+|.\d+|\d+', p.get_text().split('，')[1], re.S)
                                    if floating:
                                        floating = floating[0]
                                        if '少' in floating or '跌' in floating or '降' in floating or '下' in floating:
                                            floating = '-' + re.findall('\d+\.\d+|\d+', floating, re.S)[0] + '%'
                                        elif '加' in floating or '增' in floating or '涨' in floating or '上' in floating:
                                            floating = '+' + re.findall('\d+\.\d+|\d+', floating, re.S)[0] + '%'
                                        elif '持平' in str(p):
                                            floating = '0.00%'
                                        else:
                                            print('匹配类型错误（- +）！')
                                            print(p.get_text().split('，')[1])
                                    else:
                                        if '持平' in str(p):
                                            floating = '0.00%'
                                        else:
                                            floating = None
                                except:
                                    floating = None

                                data.update({
                                    location:
                                        {
                                            'cycle': cycle,
                                            'floating': floating
                                        }
                                })
                            else:
                                pass
                        except:
                            pass
                    if not data.get('华北地区') and not data.get('华东地区') and not data.get('华中地区'):
                        data.update({
                            '华北地区': None,
                            '华东地区': None,
                            '华中地区': None,
                        })
            except Exception as error:
                logger.warning(error)

        elif Type == "BOPP成品库存分析":
            try:
                # 地区信息
                if soup.find('div', {'class': 'xq-content'}):
                    if 'BOPP膜企成品' in str(soup.find('div', {'class': 'xq-content'}).get_text().strip()):
                        for msg in str(soup.find('div', {'class': 'xq-content'}).get_text().strip()).split('，'):
                            try:
                                days = re.findall('与上周相比([\u4e00-\u9fa5]+\d+\.\d+)', msg, re.S)
                                # print(days)
                                if days:
                                    if '上' in days[0] or '升' in days[0] or '涨' in days[0]:
                                        days = '+' + str(re.findall('\d+\.\d+', days[0], re.S)[0])
                                    if '下' in days[0] or '降' in days[0] or '跌' in days[0]:
                                        days = '-' + str(re.findall('\d+\.\d+', days[0], re.S)[0])

                                    data.update({
                                        'days': days
                                    })

                                rate = re.findall('([\u4e00-\u9fa5]+\d+\.\d+)', msg, re.S)
                                # print(rate)
                                if rate:
                                    if '上' in rate[0] or '升' in rate[0] or '涨' in rate[0]:
                                        rate = '+' + str(re.findall('\d+\.\d+', rate[0], re.S)[0]) + '%'
                                    if '下' in rate[0] or '降' in rate[0] or '跌' in rate[0]:
                                        rate = '-' + str(re.findall('\d+\.\d+', rate[0], re.S)[0]) + '%'
                                    data.update({
                                        'rate': rate
                                    })
                            except:
                                pass
            except Exception as error:
                logger.warning(error)

        if data:
            pp.pprint(data)
            return data
        else:
            return None

    # 聚丙烯
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
            return self.GetJuBingXi(date)
        else:
            time.sleep(5)
            lastDayStamp = timeStamp - 86400
            lastDayStrf = str(time.strftime("%Y-%m-%d", time.localtime(lastDayStamp)))

            if count <= 31:
                year = lastDayStrf.split('-')[0]
                month = str(int(lastDayStrf.split('-')[1]) - 1)
                day = lastDayStrf.split('-')[2]

                self.GetJuBingXi({
                    'year': year,
                    'month': month,
                    'day': day,
                })

                return self.GetSelectDate(history, lastDayStrf, count + 1)

    def GetJuBingXi(self, selectDate: dict):
        try:
            year = selectDate.get('year')
            month = selectDate.get('month')
            day = selectDate.get('day')
            jsonData = {
                'wbillWeeklyQuotes.variety': 'pp',
                'year': year,
                'month': month,
                'day': day
            }
            resp = requests.post(url=self.juBingXiApi, headers=self.juBingXiHeaders, data=urlencode(jsonData),
                                 verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                dataList = self.PaserJuBingXi(resp.text)
                if dataList:
                    if len(str(int(month) + 1)) == 1:
                        month = '0' + str(int(month) + 1)
                    jsonData.update({
                        'Type': 'PP仓单日报（日度）',
                        'hashKey': str(year) + month + str(day),
                        'tableData': dataList,
                        'month': str(int(month) + 1)
                    })
                    print(jsonData)
                    try:
                        self.articleData_coll.update_one({'hashKey': jsonData['hashKey']}, {'$set': jsonData},
                                                         upsert=True)
                    except Exception as error:
                        logger.warning(error)
                else:
                    print('没有数据！')
        except requests.exceptions.ConnectionError:
            return self.GetJuBingXi(selectDate)
        except Exception as error:
            logger.warning(error)

    @staticmethod
    def PaserJuBingXi(Html):
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
                        for num in range(len(titles)):
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
        多进程执行
    """

    # 多进程获取数据
    def CommandThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=1)

        logger.warning(self.message_coll.find({'status': None}, no_cursor_timeout=True).count())
        for info in self.message_coll.find({'status': None}):
            if Async:
                out = pool.apply_async(func=self.GetUrlFromMongo, args=(info, proxy,))  # 异步
            else:
                out = pool.apply(func=self.GetUrlFromMongo, args=(info, proxy,))  # 同步
            thread_list.append(out)
            # break

        pool.close()
        pool.join()

        # 获取输出结果
        com_list = []
        if Async:
            for p in thread_list:
                com = p.get()  # get会阻塞
                com_list.append(com)
        else:
            com_list = thread_list
        if remove_bad:
            com_list = [i for i in com_list if i is not None]
        return com_list


if __name__ == '__main__':
    ppqt = PPQita()

    # 初始化翻第一页  True 加载历史数据  False 不加载历史数据
    for info in [
        {'keyword': '塑编企业原料库存分析', 'Type': '塑编企业原料库存分析'},
        {'keyword': 'BOPP成品库存分析', 'Type': 'BOPP成品库存分析'}
    ]:
        print(info)
        ppqt.GetAllMessages(info, proxy=False, history=False, pageNum=1)

    # 多进程获取数据
    ppqt.CommandThread(proxy=False)
    print('pp-qita 获取历史数据--完成')

    # 聚丙烯数据
    # 传入查询日期 默认为昨天日期  True加载一个月历史数据  False不加载历史数据
    ppqt.GetSelectDate(history=False)
