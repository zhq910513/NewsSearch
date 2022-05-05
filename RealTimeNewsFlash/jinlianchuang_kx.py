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

logPath = os.path.join(dh + r'/Logs/jinlianchuang_kx.log')
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


class JinLianChuang:
    def __init__(self):
        db = conf.get("Mongo", "NEWSFLASHDB")
        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=db))
        self.JinLianChuang_coll = client[db]['jlc_kx']
        self.JiaoDianHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': 'visitor_type=old; 53revisit=1597935222999; SESSION=bea7f5db-cfef-4df8-8585-15146b27769d; _pk_ses.9.542d=1; 53gid2=10496607692013; visitor_type=new; 53gid0=10496607692013; 53gid1=10496607692013; 53kf_72106741_from_host=plas.315i.com; 53kf_72106741_land_page=http%253A%252F%252Fplas.315i.com%252Fplas.html; kf_72106741_land_page_ok=1; 53uvid=1; onliner_zdfq72106741=0; Hm_lvt_55585d53e3b25c979dd5e50e5021cd2c=1602403110; Hm_lpvt_55585d53e3b25c979dd5e50e5021cd2c=1602403110; 53kf_72106741_keyword=http%3A%2F%2Fplas.315i.com%2Fplas.html; _pk_id.9.542d=0d737c6193190ad0.1602403109.1.1602403381.1602403109.',
            'Host': 'plas.315i.com',
            'Pragma': 'no-cache',
            'Referer': 'http://plas.315i.com/common/goArticleList?pageIndex=2&productIds=004&columnIds=007002%2C007004%2C007003&type=0&pageId=000',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.HangQingHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': 'visitor_type=old; 53revisit=1597935222999; SESSION=bea7f5db-cfef-4df8-8585-15146b27769d; _pk_ses.9.542d=1; 53gid2=10496607692013; visitor_type=new; 53gid0=10496607692013; 53gid1=10496607692013; 53kf_72106741_from_host=plas.315i.com; 53kf_72106741_land_page=http%253A%252F%252Fplas.315i.com%252Fplas.html; kf_72106741_land_page_ok=1; 53uvid=1; onliner_zdfq72106741=0; Hm_lvt_55585d53e3b25c979dd5e50e5021cd2c=1602403110; Hm_lpvt_55585d53e3b25c979dd5e50e5021cd2c=1602403110; 53kf_72106741_keyword=http%3A%2F%2Fplas.315i.com%2Fplas.html; _pk_id.9.542d=0d737c6193190ad0.1602403109.1.1602403381.1602403109.',
            'Host': 'plas.315i.com',
            'Pragma': 'no-cache',
            'Referer': 'http://plas.315i.com/common/goArticleList?pageIndex=2&productIds=004&columnIds=007002%2C007004%2C007003&type=0&pageId=000',
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

    """
        焦点新闻
    """
    def GetJiaoDianData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.JiaoDianHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.JiaoDianParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.JinLianChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount = DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetJiaoDianData(data.get('nextPage'), dataSource, Type)
                    else:
                        pass
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetJiaoDianData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None

    @staticmethod
    def JiaoDianParser(html):
        soup = BeautifulSoup(html, 'lxml')

        data = {}
        dataList = []

        try:
            for li in soup.find('div', {'class': 'pad019'}).find_all('li'):
                try:
                    titleType = li.find_all('a')[0].find('span').get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ')
                except:
                    titleType= None

                try:
                    title = li.find_all('a')[1].get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ')
                except:
                    title = None

                try:
                    link = 'http://plas.315i.com' + li.find_all('a')[1].get('href')
                except:
                    link = None

                try:
                    uploadTime = li.find('span', {'class': 'fr'}).get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ').replace('\n', '').replace('\t', '').replace('\r', '').replace('     ', ' ').replace('                    ', ' ')
                except:
                    uploadTime = None

                dataList.append({
                    'titleType': titleType,
                    'title': title,
                    'link': link,
                    'uploadTime': uploadTime
                })
        except Exception as error:
            logger.warning(error)

        try:
            if 'gotoPage' in soup.find('div', {'id': 'page'}).find_all('a')[-2].get('onclick'):
                nextPage = 'http://plas.315i.com/common/goArticleList?pageIndex={}&productIds=004&columnIds=007002%2C007004%2C007003&type=0&pageId=000'.format(soup.find('div', {'id': 'page'}).find_all('a')[-2].get('onclick').split("'")[1])
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
        行情速递
    """
    def GetHangQingData(self, Url, dataSource, Type):
        DuplicateCount = 0
        print(Url)
        try:
            resp = requests.get(Url, headers=self.HangQingHeaders, verify=False)
            resp.encoding = 'utf-8'
            if str(resp.status_code).startswith('2'):
                data = self.HangQingParser(resp.text)
                if data:
                    if data.get('dataList'):
                        for info in data.get('dataList'):
                            info.update({
                                'type': Type,
                                'dataSource': dataSource
                            })
                            try:
                                self.JinLianChuang_coll.insert_one(info)
                            except DuplicateKeyError:
                                print('DuplicateKeyError')
                                DuplicateCount= DuplicateCount + 1
                            except Exception as error:
                                logger.warning(error)

                    if data.get('nextPage') and DuplicateCount < 5:
                        time.sleep(random.choice(range(1, 5)))
                        return self.GetHangQingData(data.get('nextPage'), dataSource, Type)
                    else:
                        pass
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetHangQingData(Url, dataSource, Type)
        except Exception as error:
            logger.warning(error)
            return None

    @staticmethod
    def HangQingParser(html):
        soup = BeautifulSoup(html, 'lxml')

        data = {}
        dataList = []

        try:
            for li in soup.find('div', {'class': 'pad019'}).find_all('li'):
                try:
                    titleType = li.find_all('a')[0].find('span').get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ')
                except:
                    titleType= None

                try:
                    title = li.find_all('a')[1].get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ')
                except:
                    title = None

                try:
                    link = 'http://plas.315i.com' + li.find_all('a')[1].get('href')
                except:
                    link = None

                try:
                    uploadTime = li.find('span', {'class': 'fr'}).get_text().strip().replace(u'\u3000', u'  ').replace(u'\xa0', u' ').replace('\n', '').replace('\t', '').replace('\r', '').replace('     ', ' ').replace('                    ', ' ')
                except:
                    uploadTime = None

                dataList.append({
                    'titleType': titleType,
                    'title': title,
                    'link': link,
                    'uploadTime': uploadTime
                })
        except Exception as error:
            logger.warning(error)

        try:
            if 'gotoPage' in soup.find('div', {'id': 'page'}).find_all('a')[-2].get('onclick'):
                nextPage = 'http://plas.315i.com/common/goArticleList?pageIndex={}&productIds=004&columnIds=002015%2C001060%2C001023%2C001024%2C001057&type=1&pageId=003'.format(soup.find('div', {'id': 'page'}).find_all('a')[-2].get('onclick').split("'")[1])
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


def run():
    jlc = JinLianChuang()

    # # 焦点新闻
    print('焦点新闻')
    url= 'http://plas.315i.com/common/goArticleList?pageIndex=1&productIds=004&columnIds=007002%2C007004%2C007003&type=0&pageId=000'
    jlc.GetJiaoDianData(url, '焦点新闻', '焦点新闻')

    # 行情速递
    print('行情速递')
    url= 'http://plas.315i.com/common/goArticleList?pageIndex=1&productIds=004&columnIds=002015%2C001060%2C001023%2C001024%2C001057&type=1&pageId=003'
    jlc.GetHangQingData(url, '行情速递', '行情速递')


if __name__ == '__main__':
    run()
