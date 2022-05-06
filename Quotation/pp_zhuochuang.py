#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import random
import sys
from multiprocessing.pool import ThreadPool

sys.path.append("../")
import configparser
import hashlib
import logging
import os
import pprint
import time
import re
from os import path
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from Cookies.proxy import HandleProxy

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/pp_zhuochuang.log'))
settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))
UsrPath = os.path.abspath(os.path.abspath(os.path.join(dh + '/Cookies/cookie.json')))

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


class PPZhuoChuang:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")
        proxydb = conf.get("Mongo", "PROXY")

        # client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=datadb))

        # cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        cookieclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        # proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        proxyclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]

        self.message_coll = client[datadb]['pp_zhuochuang_messages']
        self.articleData_coll = client[datadb]['pp_zhuochuang_articleData']
        self.pageApiUrl = 'https://www.sci99.com/search/ajax.aspx'
        self.userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        self.pageApiHeaders = {
            'authority': 'www.sci99.com',
            'method': 'POST',
            'path': '/search/ajax.aspx',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'content-length': '131',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://www.sci99.com',
            'pragma': 'no-cache',
            'referer': 'https://www.sci99.com/search/?key=PP%E8%A3%85%E7%BD%AE%E5%8A%A8%E6%80%81%E6%B1%87%E6%80%BB&siteid=0',
            'user-agent': self.userAgent,
            'x-requested-with': 'XMLHttpRequest'
        }
        self.articleHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/37665388.html',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/37665388.html',
            'upgrade-insecure-requests': '1',
            'user-agent': self.userAgent
        }
        self.titleOne = ''
        self.titleTwo = ''
        self.titleThree = ''

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

    # 获取最新数据或者历史数据
    def GetAllMessages(self, Type, pageNum=1, proxy=False, history=False):
        print('{}    第 {} 页'.format(Type, pageNum))
        try:
            jsonData = {
                'action': 'getlist',
                'keyword': Type,
                'sccid': 0,
                'pageindex': pageNum,
                'siteids': 0,
                'pubdate': '',
                'orderby': 'true'
            }
            self.pageApiHeaders.update({
                'cookie': self.cookie_coll.find_one({'name': 'zc_pp_messages'}).get('cookie'),
            })
            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url=self.pageApiUrl, headers=self.pageApiHeaders, proxies=pro,
                                         data=urlencode(jsonData), timeout=5, verify=False)
                else:
                    resp = requests.post(url=self.pageApiUrl, headers=self.pageApiHeaders, data=urlencode(jsonData),
                                         timeout=5, verify=False)
            else:
                resp = requests.post(url=self.pageApiUrl, headers=self.pageApiHeaders, data=urlencode(jsonData),
                                     timeout=5, verify=False)

            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data = self.ParseMessages(resp.json()[0])
                if data.get('dataList'):
                    if history:
                        for info in data.get('dataList'):
                            info.update({'Type': Type})

                            if Type == '丙烯下游周度开工率统计':
                                if info['FColumnName'] == '装置动态' and info["WebSite"] == "化工":
                                    # print(info)
                                    self.message_coll.update_one({'link': info['link']}, {'$set': info}, upsert=True)

                            elif Type == '塑料两油库存':
                                if info['ClassName'] == 'PP粒':
                                    # print(info)
                                    self.message_coll.update_one({'link': info['link']}, {'$set': info}, upsert=True)

                            elif Type == 'PP装置动态汇总':
                                if info['FColumnName'] == '石化动态':
                                    # print(info)
                                    self.message_coll.update_one({'link': info['link']}, {'$set': info}, upsert=True)

                            else:
                                # print(info)
                                self.message_coll.update_one({'link': info['link']}, {'$set': info}, upsert=True)

                        if pageNum < int(data.get('maxPage')):
                            # 随机休眠
                            time.sleep(random.uniform(3, 5))
                            return self.GetAllMessages(Type, pageNum + 1, proxy, history)
                    else:
                        for info in data.get('dataList'):
                            info.update({'Type': Type})

                            if Type == '丙烯下游周度开工率统计':
                                if info['FColumnName'] == '装置动态' and info["WebSite"] == "化工":
                                    if not info.get("ClassName") == "管材管件":
                                        self.message_coll.update_one({'link': info['link']}, {'$set': info},
                                                                     upsert=True)
                            elif Type == '塑料两油库存':
                                if info['ClassName'] == 'PP粒':
                                    if not info.get("ClassName") == "管材管件":
                                        self.message_coll.update_one({'link': info['link']}, {'$set': info},
                                                                     upsert=True)
                            else:
                                if not info.get("ClassName") == "管材管件":
                                    self.message_coll.update_one({'link': info['link']}, {'$set': info}, upsert=True)
        except requests.exceptions.ConnectionError:
            # 标记失效代理
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetAllMessages(Type, pageNum, True, history)
        except TimeoutError:
            # 标记失效代理
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetAllMessages(Type, pageNum, True, history)
        except Exception as error:
            logger.warning(error)
            return

    # 解析最新数据或者历史数据
    @staticmethod
    def ParseMessages(dataJson):
        data = {}
        dataList = []
        try:
            if dataJson.get('hits') and isinstance(dataJson.get('hits'), list):
                for info in dataJson.get('hits'):
                    info['link'] = info['URL'] + 'news/{}.html'.format(info['NewsKey'])
                    dataList.append(info)
                if dataList:
                    data.update({
                        'dataList': dataList
                    })

            if dataJson.get('totalPages'):
                data.update({
                    'maxPage': dataJson.get('totalPages')
                })

            if data:
                return data
        except Exception as error:
            logger.warning(error)

    # 获取每一篇文章的内容
    def GetUrlFromMongo(self, info, proxy=False):
        Type = info['Type']
        link = info['link']
        print(link)

        pubTime = info['PubTime'].split('T')[0]
        try:
            if Type == '丙烯下游周度开工率统计':
                self.articleHeaders.update({
                    'cookie': self.cookie_coll.find_one({'name': 'zc_pp_bxxy_article'}).get('cookie'),
                })
            else:
                self.articleHeaders.update({
                    'cookie': self.cookie_coll.find_one({'name': 'zc_pp_article'}).get('cookie'),
                })

            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(url=link.replace('http://', 'https://'), headers=self.articleHeaders,
                                        proxies=pro, timeout=5, verify=False)
                else:
                    resp = requests.get(url=link.replace('http://', 'https://'), headers=self.articleHeaders, timeout=5,
                                        verify=False)
            else:
                resp = requests.get(url=link.replace('http://', 'https://'), headers=self.articleHeaders, timeout=5,
                                    verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                pubTime_new, dataList = self.ParseArticle(Type, resp.text, info['link'], pubTime)
                if pubTime_new:
                    pubTime = pubTime_new
                if dataList:
                    hashKey = hashlib.md5((pubTime + link).encode("utf8")).hexdigest()
                    print(hashKey)
                    try:
                        self.articleData_coll.update_one({'hashKey': hashKey},
                                                         {'$set':
                                                             {
                                                                 'hashKey': hashKey,
                                                                 'Type': Type,
                                                                 'link': info['link'],
                                                                 'pubTime': pubTime,
                                                                 'dataList': dataList
                                                             }},
                                                         upsert=True)
                        self.message_coll.update_one({'link': info['link']}, {'$set': {'status': 1}}, upsert=True)
                    except Exception as error:
                        logger.warning(error)
            else:
                print('*{}*'.format(resp.status_code))
                self.message_coll.update_one({'link': info['link']}, {'$set': {'status': resp.status_code}},
                                             upsert=True)
        except requests.exceptions.ConnectionError:
            # 标记失效代理
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetUrlFromMongo(info, proxy)
        except TimeoutError:
            # 标记失效代理
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetUrlFromMongo(info, True)
        except Exception as error:
            logger.warning(error)
            return

        # 随机休眠
        time.sleep(random.uniform(25, 30))

    # 解析每一篇文章的内容
    def ParseArticle(self, Type, Html, link, pubTime):
        soup = BeautifulSoup(Html, 'lxml')
        dataList = []

        if Type == 'PP装置动态汇总':
            try:
                if 'Panel_Login' in Html:
                    print('登陆失效，请重新登陆获取cookie！')
                    return None, None

                pubTime_info = re.findall('(\d+-\d+-\d+ \d+:\d+:\d+)', soup.get_text(), re.S)
                if pubTime_info:
                    pubTime = pubTime_info[0]

                # 表头
                titles = [td.get_text().strip() for td in soup.find('tbody').find_all('tr')[0].find_all('td')]
                for tr in soup.find('tbody').find_all('tr')[1:]:
                    data = {}
                    try:
                        if len(tr.find_all('td')) == len(titles):
                            self.titleOne = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t', '').replace(
                                '\r', '').strip()
                            self.titleTwo = tr.find_all('td')[1].get_text().replace('\n', '').replace('\t', '').replace(
                                '\r', '').strip()
                            self.titleThree = tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                '\r', '').strip()

                            for num in range(len(titles)):
                                # if tr.find_all('td')[num].find('span'):
                                #     key = titles[num]
                                #     value = [tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()]
                                # if '停车' in value[0]:
                                #     value = 0
                                #     data.update({
                                #         key: value
                                #     })
                                # else:
                                key = titles[num]
                                value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace(
                                    '\r', '').strip()
                                # if '停车' in value:
                                #     value = 0
                                data.update({
                                    key: value
                                })
                        elif len(tr.find_all('td')) + 1 == len(titles):
                            self.titleTwo = tr.find_all('td')[0].get_text().replace('\n', '').replace('\t', '').replace(
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
                                # if '停车' in value[0]:
                                #     value = 0
                                #     data.update({
                                #        key: value
                                #     })
                                # else:
                                key = titles[num + 1]
                                value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace(
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
                                # if '停车' in value[0]:
                                #     value = 0
                                #     data.update({
                                #        key: value
                                #     })
                                # else:
                                key = titles[num + 2]
                                value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace(
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
                                #     value = [tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()]
                                # if '停车' in value[0]:
                                #     value = 0
                                #     data.update({
                                #         key: value
                                #     })
                                # else:
                                key = titles[num + 3]
                                value = tr.find_all('td')[num].get_text().replace('\n', '').replace('\t', '').replace(
                                    '\r', '').strip()
                                # if '停车' in value:
                                #     value = 0
                                data.update({
                                    key: value
                                })
                        else:
                            logger.warning('表头对不上！  {}'.format(link))
                    except:
                        pass
                    if data:
                        dataList.append(data)
                if dataList:
                    return pubTime, dataList
            except Exception as error:
                logger.warning(error)

        elif Type == '国内石化PP生产比例汇总':
            try:
                if soup.find('tbody'):
                    titles = soup.find('tbody').find_all('tr')[0].find_all('td')
                    if len(titles) == 11:
                        if len(soup.find('tbody').find_all('tr')[1].find_all('td')) == 15:
                            for tr in soup.find('tbody').find_all('tr')[2:]:
                                if len(tr.find_all('td')) == 16:
                                    try:
                                        dataList.append({
                                            '日期': tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                                '\r', '').strip(),
                                            '拉丝': tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                            '').replace(
                                                '\r', '').strip(),
                                            '均聚注塑': {
                                                '薄壁注塑': tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                                    '\r', '').strip(),
                                                '普通均聚注塑': tr.find_all('td')[3].get_text().replace('\n', '').replace(
                                                    '\t', '').replace('\r', '').strip()
                                            },
                                            '共聚注塑': {
                                                '低熔注塑': tr.find_all('td')[4].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                                    '\r', '').strip(),
                                                '中熔注塑': tr.find_all('td')[5].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                                    '\r', '').strip(),
                                                '高熔注塑': tr.find_all('td')[6].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                                    '\r', '').strip()
                                            },
                                            '纤维': {
                                                '低熔纤维': tr.find_all('td')[7].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                                    '\r', '').strip(),
                                                '高熔纤维': tr.find_all('td')[8].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                                    '\r', '').strip()
                                            },
                                            'BOPP': tr.find_all('td')[9].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            'CPP': tr.find_all('td')[10].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            '管材': {
                                                'PPR管材': tr.find_all('td')[11].get_text().replace('\n', '').replace(
                                                    '\t', '').replace('\r', '').strip(),
                                                'PPB管材': tr.find_all('td')[12].get_text().replace('\n', '').replace(
                                                    '\t', '').replace('\r', '').strip()
                                            },
                                            '透明料': tr.find_all('td')[13].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            '其他': tr.find_all('td')[14].get_text().replace('\n', '').replace('\t',
                                                                                                             '').replace(
                                                '\r', '').strip(),
                                            '停车': tr.find_all('td')[15].get_text().replace('\n', '').replace('\t',
                                                                                                             '').replace(
                                                '\r', '').strip()
                                        })
                                    except:
                                        pass
                                else:
                                    logger.warning('第三行内容数量不匹配  %s' % link)
                        elif len(soup.find('tbody').find_all('tr')[1].find_all('td')) == 11:
                            for tr in soup.find('tbody').find_all('tr')[2:]:
                                try:
                                    dataList.append({
                                        '日期': tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip(),
                                        '拉丝': tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip(),
                                        '均聚注塑': tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip(),
                                        '共聚注塑': tr.find_all('td')[3].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip()
                                        ,
                                        '纤维': tr.find_all('td')[4].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip()
                                        ,
                                        'BOPP': tr.find_all('td')[5].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip(),
                                        'CPP': tr.find_all('td')[6].get_text().replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                            '\r', '').strip(),
                                        '管材': tr.find_all('td')[7].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip()
                                        ,
                                        '透明料': tr.find_all('td')[8].get_text().replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                            '\r', '').strip(),
                                        '其他': tr.find_all('td')[9].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip(),
                                        '停车': tr.find_all('td')[10].get_text().replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                            '\r', '').strip()
                                    })
                                except:
                                    pass
                        else:
                            logger.warning('第二行表头数量不匹配  %s' % link)
                    elif len(soup.find('tbody').find_all('tr')[0].find_all('td')) == 16:
                        for tr in soup.find('tbody').find_all('tr')[1:]:
                            if len(tr.find_all('td')) == 16:
                                try:
                                    dataList.append({
                                        '日期': tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip(),
                                        '拉丝': tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip(),
                                        '均聚注塑': {
                                            '薄壁注塑': tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            '普通均聚注塑': tr.find_all('td')[3].get_text().replace('\n', '').replace('\t',
                                                                                                                '').replace(
                                                '\r', '').strip()
                                        },
                                        '共聚注塑': {
                                            '低熔注塑': tr.find_all('td')[4].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            '中熔注塑': tr.find_all('td')[5].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            '高熔注塑': tr.find_all('td')[6].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip()
                                        },
                                        '纤维': {
                                            '低熔纤维': tr.find_all('td')[7].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip(),
                                            '高熔纤维': tr.find_all('td')[8].get_text().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                                '\r', '').strip()
                                        },
                                        'BOPP': tr.find_all('td')[9].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip(),
                                        'CPP': tr.find_all('td')[10].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip(),
                                        '管材': {
                                            'PPR管材': tr.find_all('td')[11].get_text().replace('\n', '').replace('\t',
                                                                                                                '').replace(
                                                '\r', '').strip(),
                                            'PPB管材': tr.find_all('td')[12].get_text().replace('\n', '').replace('\t',
                                                                                                                '').replace(
                                                '\r', '').strip()
                                        },
                                        '透明料': tr.find_all('td')[13].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip(),
                                        '其他': tr.find_all('td')[14].get_text().replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                            '\r', '').strip(),
                                        '停车': tr.find_all('td')[15].get_text().replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                            '\r', '').strip()
                                    })
                                except:
                                    pass
                            else:
                                logger.warning('第三行内容数量不匹配  %s' % link)
                    elif len(soup.find('tbody').find_all('tr')) == 16:
                        print('纵向')
                        titles = [td.get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip() for td in
                                  soup.find('table', {'align': 'center'}).find_all('tr')[0].find_all('td')[1:]]
                        values = soup.find('tbody').find_all('tr')
                        for num in range(len(titles)):
                            try:
                                dataList.append({
                                    '日期': titles[num],
                                    '拉丝': values[1].find_all('td')[num + 1].get_text().replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                        '\r', '').strip(),
                                    '均聚注塑': {
                                        '薄壁注塑': values[2].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                                            '\t', '').replace('\r', '').strip(),
                                        '普通均聚注塑': values[3].find_all('td')[num + 1].get_text().replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '').replace(
                                            '\r', '').strip()
                                    },
                                    '共聚注塑': {
                                        '低熔注塑': values[4].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                                            '\t', '').replace('\r', '').strip(),
                                        '中熔注塑': values[5].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                                            '\t', '').replace('\r', '').strip(),
                                        '高熔注塑': values[6].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                                            '\t', '').replace('\r', '').strip()
                                    },
                                    '纤维': {
                                        '低熔纤维': values[7].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                                            '\t', '').replace('\r', '').strip(),
                                        '高熔纤维': values[8].find_all('td')[num + 1].get_text().replace('\n', '').replace(
                                            '\t', '').replace('\r', '').strip()
                                    },
                                    'BOPP': values[9].find_all('td')[num + 1].get_text().replace('\n', '').replace('\t',
                                                                                                                   '').replace(
                                        '\r', '').strip(),
                                    'CPP': values[10].find_all('td')[num + 1].get_text().replace('\n', '').replace('\t',
                                                                                                                   '').replace(
                                        '\r', '').strip(),
                                    '管材': {
                                        'PPR管材': values[11].find_all('td')[num + 1].get_text().replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '').replace(
                                            '\r', '').strip(),
                                        'PPB管材': values[12].find_all('td')[num + 1].get_text().replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '').replace(
                                            '\r', '').strip()
                                    },
                                    '透明料': values[13].find_all('td')[num + 1].get_text().replace('\n', '').replace('\t',
                                                                                                                   '').replace(
                                        '\r', '').strip(),
                                    '其他': values[14].find_all('td')[num + 1].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                        '\r', '').strip(),
                                    '停车': values[15].find_all('td')[num + 1].get_text().replace('\n', '').replace('\t',
                                                                                                                  '').replace(
                                        '\r', '').strip()
                                })
                            except:
                                pass
                    else:
                        logger.warning('第一行表头数量不匹配  %s' % link)
            except Exception as error:
                logger.warning(error)

        elif Type == '聚丙烯PP粉料产量':
            try:
                if soup.find('span', {'id': 'zoom'}):
                    # 月份
                    try:
                        month = re.findall('\d+年\d+月', str(soup.find('span', {'id': 'zoom'})), re.S)[0]
                    except:
                        try:
                            month = re.findall('\d+年\d+月', str(soup.find('span', {'id': 'zoom'})), re.S)[0]
                        except:
                            month = None

                    # 产量
                    try:
                        count = re.findall('聚丙烯粉料产量在(.*?)吨', str(soup.find('span', {'id': 'zoom'})), re.S)[0]
                    except:
                        count = None

                    # 百分比
                    try:
                        rate = re.findall('开工在(\d+\.\d+|\d+)', str(soup.find('span', {'id': 'zoom'})), re.S)[0]
                    except:
                        rate = None

                    dataList.append({
                        'date': month,
                        'count': count,
                        'rate': rate
                    })
            except Exception as error:
                logger.warning(error)

        elif Type == '神华PP竞拍结果':
            try:
                if soup.find('tbody'):
                    # print('center')
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
                                logger.warning('表头对不上！  {}'.format(tr))
                        except:
                            pass
                        if data:
                            dataList.append(data)
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                print('error')
                logger.warning(error)

        elif Type == '塑料两油库存':
            dataList = {}
            try:
                if soup.find('span', {'id': 'zoom'}):
                    # 产量
                    try:
                        count = re.findall('今日两油库存在(.*?)吨', str(soup.find('span', {'id': 'zoom'})), re.S)[0]
                    except:
                        count = None

                    dataList.update({
                        'count': count,
                        'dateTime': pubTime
                    })

            except Exception as error:
                logger.warning(error)

        elif Type == '聚丙烯粉料及上游丙烯价格一览':
            try:
                if soup.find('tbody'):
                    for tr in soup.find('tbody').find_all('tr')[1:]:
                        if len(tr.find_all('td')) == 3:
                            if '山东地区' in tr.find_all('td')[0].get_text().replace('\n', '').replace('\t', '').replace(
                                    '\r', '').strip() or '江浙地区' in tr.find_all('td')[0].get_text().replace('\n',
                                                                                                           '').replace(
                                '\t', '').replace('\r', '').strip():
                                try:
                                    dataList.append({
                                        '地区': tr.find_all('td')[0].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                            '\r', '').strip(),
                                        'PP粉': tr.find_all('td')[1].get_text().replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                            '\r', '').strip(),
                                        '丙烯单体': tr.find_all('td')[2].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip(),
                                    })
                                except:
                                    pass
                            else:
                                logger.warning('数据表头不匹配   %s' % link)
                else:
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '无表结构'}}, upsert=True)
                    print('出现新的页面结构！')
            except Exception as error:
                logger.warning(error)

        elif Type == '丙烯下游周度开工率统计':
            try:
                if soup.find('tbody'):
                    if len(soup.find('tbody').find_all('tr')[0].find_all('td')) == 9:
                        for tr in soup.find('tbody').find_all('tr')[1:]:
                            if len(tr.find_all('td')) == 9 and '环比' not in tr.find_all('td')[0].get_text().replace('\n',
                                                                                                                   '').replace(
                                '\t', '').replace('\r', '').strip():
                                dataList.append({
                                    '日期': tr.find_all('td')[0].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    '粉料': tr.find_all('td')[1].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    'PO': tr.find_all('td')[2].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    '正丁醇': tr.find_all('td')[3].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    '丙烯腈': tr.find_all('td')[4].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    '酚酮': tr.find_all('td')[5].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    '丙烯酸': tr.find_all('td')[6].get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip(),
                                    '丙烯法ECH': tr.find_all('td')[7].get_text().replace('\n', '').replace('\t',
                                                                                                        '').replace(
                                        '\r', '').strip()
                                })
                    else:
                        self.message_coll.update_one({'link': link}, {'$set': {'status': '没有表结构'}}, upsert=True)
                        logger.warning('没有表结构   %s' % link)
                else:
                    logger.warning('没有找到数据，可能cookies过期   %s' % link)
                    self.message_coll.update_one({'link': link}, {'$set': {'status': 404}}, upsert=True)
            except Exception as error:
                logger.warning(error)

        elif Type == '塑膜收盘价格表':
            try:
                date = re.findall('\d+-\d+-\d+', soup.find('div', {'style': 'float: left'}).get_text(), re.S)[0]

                if '产品' in soup.find('tbody').find_all('tr')[0].find_all('td')[0].get_text().strip() \
                        and '规' in soup.find('tbody').find_all('tr')[0].find_all('td')[1].get_text().strip() \
                        and ('日' in soup.find('tbody').find_all('tr')[0].find_all('td')[2].get_text().strip()
                             or '-' in soup.find('tbody').find_all('tr')[0].find_all('td')[2].get_text().strip()):
                    for tr in soup.find('tbody').find_all('tr')[1:]:
                        dataList.append({
                            '日期': date,
                            '产品': tr.find_all('td')[0].get_text().strip(),
                            '规格': tr.find_all('td')[1].get_text().strip(),
                            '价格': tr.find_all('td')[2].get_text().strip()
                        })
                elif '企业名称' in soup.find('tbody').find_all('tr')[0].find_all('td')[0].get_text().strip():
                    self.message_coll.update_one({'link': link}, {'$set': {'status': '企业名称'}}, upsert=True)
            except Exception as error:
                logger.warning(error)

        else:
            print('没有这个关键词类型   %s' % Type)

        if dataList:
            return pubTime, dataList
        else:
            return None, None

    # 多进程获取数据
    def CommandThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=5)

        for info in self.message_coll.find({'status': None, '$nor': [{"ClassName": "管材管件"}]}):
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


def run():
    pp = PPZhuoChuang()

    # 初始化翻第一页  True 加载历史数据  False 不加载历史数据
    for word in [
        'PP装置动态汇总',
        '国内石化PP生产比例汇总',
        '聚丙烯PP粉料产量',
        '神华PP竞拍结果',
        '塑料两油库存',
        '聚丙烯粉料及上游丙烯价格一览',
        '丙烯下游周度开工率统计',
        '塑膜收盘价格表'
    ]:
        # pass
        pp.GetAllMessages(word, 1, proxy=False, history=False)

    # 获取文章数据
    pp.CommandThread(proxy=False)


if __name__ == '__main__':
    run()
