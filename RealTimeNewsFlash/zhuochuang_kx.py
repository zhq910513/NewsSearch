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
pp=pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)

logPath = os.path.join(dh + r'/Logs/zhuochuang_kx.log')
settingPath = os.path.join(dh + r'/Settings.ini')

if not os.path.isfile(logPath):
    open(logPath,'w')

logger = logging.getLogger(logPath)
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# 读ini文件
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")


class ZhuoChuang:
    def __init__(self):
        db = conf.get("Mongo", "NEWSFLASHDB")
        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=db))
        self.ZhuoChuang_coll = client[db]['zc_kx']
        self.ShiDianHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=674_691_4489_4500_4696&fid=32&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928; route=5381fa73df88cce076c9e01d13c9b378; ASP.NET_SessionId=vkjs3glwifkndaigpmcpwgd3; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; CNZZDATA1262021642=1910705914-1600878992-%7C1600878992; STATReferrerIndexId=1; isCloseOrderZHLayer=0; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fplas.chem99.com%2Fchannel%2Ftongyong%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1600879162; pageViewNum=5; STATcUrl=',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/674_4696_691_4489_4500-0-0-0-32.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
        self.ReDianHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=643_670_684_4497_4510_4539_4553_4579_4700&fid=32&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928; route=5381fa73df88cce076c9e01d13c9b378; ASP.NET_SessionId=vkjs3glwifkndaigpmcpwgd3; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; CNZZDATA1262021642=1910705914-1600878992-%7C1600878992; STATReferrerIndexId=1; isCloseOrderZHLayer=0; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fplas.chem99.com%2Fchannel%2Ftongyong%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; STATcUrl=; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1600883560; pageViewNum=17',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/670_4579_684_643_4497_4510_4539_4553_4700-0-0-0-32.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
        self.JiShiHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=604_669_673_675_680_682_683_690_694_696_700_4492_4493_4499_4503_4504_4530_4531_4532_4544_4546_4547_4684_4695_4699_4702&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928; route=5381fa73df88cce076c9e01d13c9b378; ASP.NET_SessionId=vkjs3glwifkndaigpmcpwgd3; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; STATReferrerIndexId=1; isCloseOrderZHLayer=0; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fplas.chem99.com%2Fchannel%2Ftongyong%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; CNZZDATA1262021642=1910705914-1600878992-%7C1600884432; STATcUrl=; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1600884709; pageViewNum=27',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/669_675_673_4702_604_4699_682_680_683_690_696_694_4493_700_4492_4504_4499_4503_4531_4530_4532_4544_4546_4547_4695_4684-0-0-0-0.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
        self.MeiRiHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=676_686_697_4508_4536_4552_4701&sname=%e6%97%a9%e9%97%b4%e6%8f%90%e7%a4%ba&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928; route=5381fa73df88cce076c9e01d13c9b378; ASP.NET_SessionId=vkjs3glwifkndaigpmcpwgd3; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; STATReferrerIndexId=1; isCloseOrderZHLayer=0; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fplas.chem99.com%2Fchannel%2Ftongyong%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; CNZZDATA1262021642=1910705914-1600878992-%7C1600884432; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1600885054; pageViewNum=30; STATcUrl=',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/s676_686_4701_697_4508_4536_4552-0-D4E7BCE4CCE1CABE.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
        self.GuanChaHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=673_683_694_4492_4503_4532_4547_4699&k=1&sname=%e6%97%a5%e8%af%84&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928; route=5381fa73df88cce076c9e01d13c9b378; ASP.NET_SessionId=vkjs3glwifkndaigpmcpwgd3; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; STATReferrerIndexId=1; isCloseOrderZHLayer=0; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fplas.chem99.com%2Fchannel%2Ftongyong%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; CNZZDATA1262021642=1910705914-1600878992-%7C1600884432; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1600885917; pageViewNum=34; STATcUrl=',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/s673_4699_694_4492_4503_4547_4532_683-1-C8D5C6C0.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
        self.TongJiHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=2&sid=677_699_4703&k=1&sname=%e4%ba%a7%e9%87%8f&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928; route=5381fa73df88cce076c9e01d13c9b378; ASP.NET_SessionId=vkjs3glwifkndaigpmcpwgd3; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; STATReferrerIndexId=1; isCloseOrderZHLayer=0; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fplas.chem99.com%2Fchannel%2Ftongyong%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; CNZZDATA1262021642=1910705914-1600878992-%7C1600884432; STATcUrl=; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1600887470; pageViewNum=52',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/s677_4703_699-1-B2FAC1BF.html',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
        self.ShangYouHeaders = {
            'authority': 'plas.chem99.com',
            'method': 'GET',
            'path': '/news/?page=3&sid=612_614_634_4651_4670_4673_4679&siteid=3',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'guid=a56c4158-9ef5-5b67-53d8-e682a79693ea; UM_distinctid=174bbd211323ea-069047fbe96203-7a1b34-ffc00-174bbd211334d1; ASP.NET_SessionId=jhv5vcmonfexdrvopfyydssx; CNZZDATA1262021642=1910705914-1600878992-%7C1601630291; STATReferrerIndexId=1; isCloseOrderZHLayer=0; href=https%3A%2F%2Fplas.chem99.com%2Fnews%2F%3Fpage%3D1%26sid%3D674_691_4489_4500_4696%26fid%3D32%26siteid%3D3; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; Hm_lvt_f80092420c79d7f5d2822acdb956aea2=1599201957,1599237928,1601632080; Hm_lpvt_f80092420c79d7f5d2822acdb956aea2=1601632080; Hm_lvt_1a13910eeee71164d28321e01b28d926=1600878940,1601630522,1601632086; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; route=5381fa73df88cce076c9e01d13c9b378; STATcUrl=; Hm_lpvt_1a13910eeee71164d28321e01b28d926=1601635256; pageViewNum=103',
            'pragma': 'no-cache',
            'referer': 'https://plas.chem99.com/news/?page=2&sid=612_614_634_4651_4670_4673_4679&siteid=3',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
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
            for li in soup.find('div', {'class': 'news_content'}).find_all('li'):
                try:
                    title = li.find('a', {'class': 'content w570'}).get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ')
                except:
                    title = None

                try:
                    link = li.find('a', {'class': 'content w570'}).get('href')
                    if link.endswith('.html'):
                        link = 'https://plas.chem99.com/news/' + link
                    else:
                        link = None
                except:
                    link = None

                try:
                    uploadTime = li.find('a', {'class': 'date'}).get_text()
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
            for a in soup.find('div', {'id': 'MyPagerList'}).find_all('a'):
                if '下一页' in a.get_text():
                    nextPage = 'https://plas.chem99.com' + a.get('href')
                    break
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
        视点
    """
    def GetShiDianData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url,  headers=self.ShiDianHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetShiDianData(data.get('nextPage'), category, dataSource, Type)
                    else:pass
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetShiDianData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        热点
    """
    def GetReDianData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url,  headers=self.ReDianHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetReDianData(data.get('nextPage'), category, dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetReDianData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        即时
    """
    def GetJiShiData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url,  headers=self.JiShiHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetJiShiData(data.get('nextPage'), category, dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetJiShiData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        每日
    """
    def GetMeiRiData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url,  headers=self.MeiRiHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetMeiRiData(data.get('nextPage'), category, dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetMeiRiData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        观察
    """
    def GetGuanChaData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)

        try:
            resp = requests.get(Url,  headers=self.GuanChaHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetGuanChaData(data.get('nextPage'), category, dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetGuanChaData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        统计
    """
    def GetTongJiData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)

        try:
            resp = requests.get(Url,  headers=self.TongJiHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetTongJiData(data.get('nextPage'), category, dataSource, Type)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetTongJiData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


    """
        上游动态
    """
    def GetShangYouData(self, Url, category, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url,  headers=self.ShangYouHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HtmlParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'category': category,
                                'dataSource': dataSource,
                                'type': Type
                            })
                            try:
                                self.ZhuoChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetShangYouData(data.get('nextPage'), category, dataSource, Type)
                    else:pass
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetShangYouData(Url, category, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None


def run():
    zc = ZhuoChuang()

    # 卓创视点
    print('卓创视点')
    for info in [{'url': 'https://plas.chem99.com/news/?page=1&sid=674_691_4489_4500_4696&fid=32&siteid=3', 'category': '通用塑料'},
                 {'url': 'https://plas.chem99.com/news/5618_5617_613-0-0-0-32.html', 'category': '工程塑料'}]:
        zc.GetShiDianData(info['url'], info['category'], '卓创视点', '卓创视点')

    # 热点聚焦
    print('热点聚焦')
    for info in [{'url': 'https://plas.chem99.com/news/?page=1&sid=643_670_684_4497_4510_4539_4553_4579_4700&fid=32&siteid=3', 'category': '通用塑料'},
                 {'url': 'https://plas.chem99.com/news/4341_4667_4672_4656-0-0-0-32.html', 'category': '工程塑料'}]:
        zc.GetReDianData(info['url'], info['category'], '热点聚焦', '热点聚焦')

    # 即时资讯
    print('即时资讯')
    for info in [{'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=669&siteid=3', 'type': 'PE'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4702&siteid=3', 'type': 'PP粒'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=682&siteid=3', 'type': 'PP粉'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=690&siteid=3', 'type': 'PVC'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=700&siteid=3', 'type': 'PS'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4499&siteid=3', 'type': 'ABS'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4530&siteid=3', 'type': 'EPS'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4544&siteid=3', 'type': 'EVA'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/channel/metallocene/?page=1', 'type': '茂金属'},
                 {'category': '通用塑料', 'url': 'https://plas.chem99.com/channel/poe/?page=1', 'type': 'POE'},

                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4658&siteid=3', 'type': 'PC'},
                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4650&k=1&sname=PA6&siteid=3', 'type': 'PA6'},
                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4650&k=1&sname=PA66&siteid=3', 'type': 'PA66'},
                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4663&siteid=3', 'type': 'PET'},
                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4678&siteid=3', 'type': 'PBT'},
                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=616&siteid=3', 'type': 'PMMA'},
                 {'category': '工程塑料', 'url': 'https://plas.chem99.com/news/?page=1&sid=4669&siteid=3', 'type': 'POM'},
                 ]:
        zc.GetJiShiData(info['url'], info['category'], '即时资讯', info['type'])

    # 每日关注
    print('每日关注')
    for info in [{'url': 'https://plas.chem99.com/news/?page=1&sid=676_686_697_4508_4536_4552_4701&sname=%e6%97%a9%e9%97%b4%e6%8f%90%e7%a4%ba&siteid=3', 'category': '通用塑料'}]:
        zc.GetMeiRiData(info['url'], info['category'], '每日关注', '每日关注')

    # 本网观察
    print('本网观察')
    for info in [{'url': 'https://plas.chem99.com/news/?page=1&sid=673_683_694_4492_4503_4532_4547_4699&k=1&sname=%E6%97%A5%E8%AF%84&siteid=3', 'category': '通用塑料'},
                 {'url': 'https://plas.chem99.com/news/?page=1&sid=673_683_694_4492_4503_4532_4547_4699&k=1&sname=%E5%91%A8%E8%AF%84&siteid=3', 'category': '通用塑料'},
                 {'url': 'https://plas.chem99.com/news/?page=1&sid=673_683_694_4492_4503_4532_4547_4699&k=1&sname=%E6%9C%88%E8%AF%84&siteid=3', 'category': '通用塑料'}]:
        zc.GetGuanChaData(info['url'], info['category'], '本网观察', '本网观察')

    # 数据统计
    print('数据统计')
    for info in [{'url': 'https://plas.chem99.com/news/?page=1&sid=677_699_4703&k=1&sname=%E4%BA%A7%E9%87%8F&siteid=3', 'category': '通用塑料', 'type': '产量'},
                 {'url': 'https://plas.chem99.com/news/?page=1&sid=671_677_687_693_699_4490_4501_4533_4545_4697_4703&k=1&sname=%E8%BF%9B%E5%8F%A3&siteid=3', 'category': '通用塑料', 'type': '进口'},
                 {'url': 'https://plas.chem99.com/news/?page=1&sid=671_677_687_693_699_4490_4501_4533_4545_4697_4703&k=1&sname=%E5%87%BA%E5%8F%A3&siteid=3', 'category': '通用塑料', 'type': '出口'},
                 {'url': 'https://plas.chem99.com/news/?page=1&sid=671_677_687_693_699_4490_4501_4533_4545_4697_4703&k=1&sname=%E5%88%B6%E5%93%81%E4%BA%A7%E9%87%8F&siteid=3', 'category': '通用塑料', 'type': '制品产量'},

                 {'url': 'https://plas.chem99.com/news/?page=1&sid=4654_4661_4664_4674_4680&k=1&sname=%E8%BF%9B%E5%8F%A3&siteid=3', 'category': '工程塑料', 'type': '进口'},
                 {'url': 'https://plas.chem99.com/news/?page=1&sid=4654_4661_4664_4674_4680&k=1&sname=%E5%87%BA%E5%8F%A3&siteid=3', 'category': '工程塑料', 'type': '出口'},]:
        zc.GetTongJiData(info['url'], info['category'], '数据统计', info['type'])

    # 上游动态
    print('上游动态')
    for info in [{'url': 'https://plas.chem99.com/news/?page=1&sid=612_614_634_4651_4670_4673_4679&siteid=3', 'category': '工程塑料'}]:
        zc.GetShangYouData(info['url'], info['category'], '上游动态', '上游动态')


if __name__ == '__main__':
    run()
