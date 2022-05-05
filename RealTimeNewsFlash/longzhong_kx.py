#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

import configparser
import logging
import os
import pprint
import random
import time
from os import path

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)

logPath = os.path.join(dh + r'/Logs/longzhong_kx.log')
settingPath = os.path.join(dh + r'/Settings.ini')

if not os.path.isfile(logPath):
    open(logPath, 'w')

logger = logging.getLogger(logPath)
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# 读ini文件
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")

# 搜索规则 ：每次启动从第一页获取，如果页面中超过五条与数据库重复，则停止；每次访问休眠1~5秒


class LongZhong:
    def __init__(self):
        db = conf.get("Mongo", "NEWSFLASHDB")
        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=db))
        self.LongZhong_coll = client[db]['lz_kx']
        self.ZaoJianHeaders = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': '_member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkeXlRSW1USGdieHRLenM5R3RXeGYuT2Fic2dGNVhkZHA0ZGhXWmUzM2M1a1pzU2lDaEVzQ3EiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYwMjA2NTE3NywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE1OTk0NzMxNzcsImp0aSI6IjYzOTA3MDYxLTE1YTUtNDZmOS05NDY4LWU5NjljNTlkMzk0NSIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ._3693UahPc3rfdyYdd8zRybhfR-9GY7OFgimy-ME1H0; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1600065790,1600142998,1600622933,1602040320; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1602040320; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1600065790,1600142998,1600622933,1602040320; oilchem_land_url=https://list.oilchem.net/102/4126/1.html; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1602040376',
        'Host': 'list.oilchem.net',
        'Pragma': 'no-cache',
        'Referer': 'https://list.oilchem.net/102/4126/1.html',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.ReDianHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '_member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkeXlRSW1USGdieHRLenM5R3RXeGYuT2Fic2dGNVhkZHA0ZGhXWmUzM2M1a1pzU2lDaEVzQ3EiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYwMjA2NTE3NywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE1OTk0NzMxNzcsImp0aSI6IjYzOTA3MDYxLTE1YTUtNDZmOS05NDY4LWU5NjljNTlkMzk0NSIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ._3693UahPc3rfdyYdd8zRybhfR-9GY7OFgimy-ME1H0; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1600065790,1600142998,1600622933,1602040320; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1602040320; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1600065790,1600142998,1600622933,1602040320; oilchem_land_url=https://list.oilchem.net/102/4126/1.html; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1602054471',
            'Host': 'list.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://list.oilchem.net/102/498/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.CaoPanHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '_member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkeXlRSW1USGdieHRLenM5R3RXeGYuT2Fic2dGNVhkZHA0ZGhXWmUzM2M1a1pzU2lDaEVzQ3EiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYwMjA2NTE3NywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE1OTk0NzMxNzcsImp0aSI6IjYzOTA3MDYxLTE1YTUtNDZmOS05NDY4LWU5NjljNTlkMzk0NSIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ._3693UahPc3rfdyYdd8zRybhfR-9GY7OFgimy-ME1H0; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1600065790,1600142998,1600622933,1602040320; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1602040320; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1600065790,1600142998,1600622933,1602040320; oilchem_land_url=https://list.oilchem.net/102/4126/1.html; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1602055739',
            'Host': 'list.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://plas.oilchem.net/102/4130/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.HangQingHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '_member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkeXlRSW1USGdieHRLenM5R3RXeGYuT2Fic2dGNVhkZHA0ZGhXWmUzM2M1a1pzU2lDaEVzQ3EiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYwMjA2NTE3NywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE1OTk0NzMxNzcsImp0aSI6IjYzOTA3MDYxLTE1YTUtNDZmOS05NDY4LWU5NjljNTlkMzk0NSIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ._3693UahPc3rfdyYdd8zRybhfR-9GY7OFgimy-ME1H0; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1600065790,1600142998,1600622933,1602040320; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1602040320; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1600065790,1600142998,1600622933,1602040320; oilchem_land_url=https://list.oilchem.net/102/4126/1.html; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1602057589',
            'Host': 'plas.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://plas.oilchem.net/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.ShuJuHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '_member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkeXlRSW1USGdieHRLenM5R3RXeGYuT2Fic2dGNVhkZHA0ZGhXWmUzM2M1a1pzU2lDaEVzQ3EiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYwMjA2NTE3NywidXNlcklkIjoxMTExNDMzLCJpYXQiOjE1OTk0NzMxNzcsImp0aSI6IjYzOTA3MDYxLTE1YTUtNDZmOS05NDY4LWU5NjljNTlkMzk0NSIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ._3693UahPc3rfdyYdd8zRybhfR-9GY7OFgimy-ME1H0; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1600065790,1600142998,1600622933,1602040320; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1602040320; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1600065790,1600142998,1600622933,1602040320; oilchem_land_url=https://list.oilchem.net/102/4126/1.html; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1602061159',
            'Host': 'plas.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://plas.oilchem.net/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.JinRiHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '_member_user_tonken_=0; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://www.oilchem.net/; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1600142998,1600622933,1602040320,1602401759; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1602401759; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1600142998,1600622933,1602040320,1602401759; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1602402300',
            'Host': 'list.oilchem.net',
            'Pragma': 'no-cache',
            'Referer': 'https://list.oilchem.net/1/2/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

    # 获取 代理
    @staticmethod
    def GetProxy():
        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "HRKI956X4E8H9V9D"
        proxyPass = "003A0780415B418C"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        proxies = {
            "http": proxyMeta,
            "https": proxyMeta,
        }
        return proxies

    @staticmethod
    def HtmlParser(html):
        soup = BeautifulSoup(html, 'lxml')

        data = {}
        dataList = []

        try:
            for li in soup.find_all('li', {'class': 'clearfix'}):
                try:
                    title = li.find('a').get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ')
                except:
                    title = None

                try:
                    link = li.find('a').get('href')
                except:
                    link = None

                try:
                    uploadTime = li.find('span').get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ').replace('\n', '').replace('\t', '').replace('\r', '').replace('     ', ' ').replace('                    ', ' ')
                except:
                    uploadTime = None

                dataList.append({
                    'title': title,
                    'link': link,
                    'uploadTime': uploadTime
                })
        except Exception as error:
            logger.warning(error)

        try:
            if soup.find('ul', {'class': 'pages'}).find('li', {'class': 'text next'}):
                if '下一页' in soup.find('ul', {'class': 'pages'}).find('li', {'class': 'text next'}).find('a').get_text():
                    nextPage = soup.find('ul', {'class': 'pages'}).find('li', {'class': 'text next'}).find('a').get('href')
                else:
                    nextPage = None
            else:
                nextPage = None
        except Exception as error:
            nextPage = None
            logger.warning(error)

        if nextPage:
            data.update({
                'nextPage': nextPage
            })

        if dataList:
            data.update({
                'dataList': dataList
            })

        if data:
            return data


    """
        早间提示
    """
    def GetZaoJianData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.ZaoJianHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.LongZhong_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetZaoJianData(data.get('nextPage'), dataSource, Type)
                    else:
                        pass
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetZaoJianData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        热点聚焦
    """
    def GetReDianData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.ReDianHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.LongZhong_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetReDianData(data.get('nextPage'), dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetReDianData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        操盘必读
    """
    def GetCaoPanData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.CaoPanHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.LongZhong_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetCaoPanData(data.get('nextPage'), dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetCaoPanData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        行情中心
    """
    def GetHangQingData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.HangQingHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.LongZhong_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetHangQingData(data.get('nextPage'), dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetHangQingData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        数据中心
    """
    def GetShuJuData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.ShuJuHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.LongZhong_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetShuJuData(data.get('nextPage'), dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetShuJuData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        今日原油
    """
    def GetJinRiData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.JinRiHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.LongZhong_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetJinRiData(data.get('nextPage'), dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetJinRiData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


def run():
    lz = LongZhong()

    # 早间提示
    print('早间提示')
    for info in [{'url': 'https://list.oilchem.net/102/4126/1.html', 'type': '早参'},
                 {'url': 'https://list.oilchem.net/102/4127/1.html', 'type': '中参'}]:
        lz.GetZaoJianData(info['url'], '早间提示', info['type'])

    # 热点聚焦
    print('热点聚焦')
    url = 'https://list.oilchem.net/102/498/1.html'
    lz.GetReDianData(url, '热点聚焦', '热点')

    # 操盘必读
    print('操盘必读')
    for info in [{'url': 'https://list.oilchem.net/102/4129/1.html', 'Type': '日评'},
                 {'url': 'https://list.oilchem.net/102/4130/1.html', 'Type': '周评'},
                 {'url': 'https://list.oilchem.net/102/4131/1.html', 'Type': '月评'}]:
        lz.GetCaoPanData(info['url'], '操盘必读', info['Type'])

    # 行情中心
    print('行情中心')
    for info in [{'url': 'https://list.oilchem.net/102/15911/1.html', 'type': 'PE'},
                 {'url': 'https://list.oilchem.net/102/15912/1.html', 'type': 'PP粒'},
                 {'url': 'https://list.oilchem.net/102/15913/1.html', 'type': 'PP粉'},
                 {'url': 'https://list.oilchem.net/102/15914/1.html', 'type': 'PVC'},
                 {'url': 'https://list.oilchem.net/102/15915/1.html', 'type': 'PS'},
                 {'url': 'https://list.oilchem.net/102/15916/1.html', 'type': 'EPS'},
                 {'url': 'https://list.oilchem.net/102/15917/1.html', 'type': 'ABS'},
                 {'url': 'https://list.oilchem.net/102/15918/1.html', 'type': 'EVA'},
                 {'url': 'https://list.oilchem.net/102/15919/1.html', 'type': 'PA'},
                 {'url': 'https://list.oilchem.net/102/15920/1.html', 'type': 'PC'},
                 {'url': 'https://list.oilchem.net/102/15922/1.html', 'type': 'PMMA'},
                 {'url': 'https://list.oilchem.net/102/15923/1.html', 'type': 'POM'},
                 {'url': 'https://list.oilchem.net/102/15924/1.html', 'type': 'PBT'},
                 ]:
        lz.GetHangQingData(info['url'], '行情中心', info['type'])

    # 数据中心
    print('数据中心')
    for info in [{'url': 'https://list.oilchem.net/102/15939/1.html', 'type': 'PE'},
                 {'url': 'https://list.oilchem.net/102/15940/1.html', 'type': 'PP粒'},
                 {'url': 'https://list.oilchem.net/102/15941/1.html', 'type': 'PVC'},
                 {'url': 'https://list.oilchem.net/102/15942/1.html', 'type': 'PS'},
                 {'url': 'https://list.oilchem.net/102/15943/1.html', 'type': 'EPS'},
                 {'url': 'https://list.oilchem.net/102/15944/1.html', 'type': 'ABS'},
                 {'url': 'https://list.oilchem.net/102/15945/1.html', 'type': 'EVA'},
                 {'url': 'https://list.oilchem.net/102/15946/1.html', 'type': 'PC'},
                 {'url': 'https://list.oilchem.net/102/15947/1.html', 'type': 'PMMA'},
                 {'url': 'https://list.oilchem.net/102/15948/1.html', 'type': 'POM'},
                 {'url': 'https://list.oilchem.net/102/15949/1.html', 'type': 'PBT'},
                 ]:
        lz.GetShuJuData(info['url'], '数据中心', info['type'])

    # 今日原油
    print('今日原油')
    url = 'https://list.oilchem.net/1/2/1.html'
    lz.GetJinRiData(url, '今日原油', '今日原油')


if __name__ == '__main__':
    run()
