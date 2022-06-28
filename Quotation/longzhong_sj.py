#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import random
import sys

sys.path.append("../")
import configparser
import json
import logging
import os
import pprint
import re
import time
import hashlib
from os import path
from urllib.parse import urlencode, unquote, quote

import pandas as pd
import requests
from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient
import pymysql
from pymysql.err import IntegrityError
import warnings

warnings.filterwarnings('ignore')
requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/longzhong_sj.log'))
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


# 搜索规则 ：每次启动从数据库获取 categoryUrl ，结束时间自动设置为当天， 下载完还原 categoryUrl 状态


class LongZhong:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")

        # client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=datadb))

        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        cookieclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        self.category_coll = client[datadb]['lz_sj_category']
        self.categoryData_coll = client[datadb]['lz_sj_categoryData']
        self.downloadDetail_coll = client[datadb]['lz_sj_downloadData']

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

        # 请求头信息
        self.userAgent = UserAgent().random
        self.categoryDataHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'dc.oilchem.net',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.downloadDetailUrl = 'https://dc.oilchem.net/price_search/doExportZip.htm?'
        self.downloadDetailHeaders = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'dc.oilchem.net',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

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

    """
        获取主类目
    """

    # 获取所有主类目
    def GetMainCategory(self):
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '14',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': self.cookie_coll.find_one({'name': 'lz_sj_category'}).get('cookie'),
            'Host': 'dc.oilchem.net',
            'Origin': 'https://dc.oilchem.net',
            'Pragma': 'no-cache',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        for channel in [
            {'templateType': 'templateType=6&flagAndTemplate=1-6;2-7;3-4;6-null', 'channelId': 1775, 'oneName': '塑料',
             'twoName': '通用塑料'},
            {'templateType': 'templateType=2&flagAndTemplate=1-2;2-2', 'channelId': 1785, 'oneName': '塑料',
             'twoName': '工程塑料'},
            {'templateType': 'templateType=1&flagAndTemplate=1-1', 'channelId': 4706, 'oneName': '塑料',
             'twoName': '改性塑料'},
            {'templateType': 'templateType=2&flagAndTemplate=1-2;2-2', 'channelId': 1792, 'oneName': '塑料',
             'twoName': '塑料制品'},
            {'templateType': 'templateType=6&flagAndTemplate=1-6;2-2;3-4', 'channelId': 1801, 'oneName': '塑料',
             'twoName': '塑料管材'},
            {'templateType': 'templateType=5&flagAndTemplate=2-5', 'channelId': 1804, 'oneName': '塑料',
             'twoName': '再生塑料'},
            {'templateType': 'templateType=2&flagAndTemplate=1-2;2-7', 'channelId': 4164, 'oneName': '塑料',
             'twoName': '可降解材料'}
        ]:
            try:
                resp = requests.post(
                    url='https://dc.oilchem.net/priceCompany/channellist.htm',
                    headers=headers,
                    data=urlencode({'channelId': channel['channelId']}),
                    timeout=5, verify=False)
                if resp.status_code == 200:
                    if resp.json().get('message') == '查询成功' and isinstance(resp.json().get('dtos'), list):
                        for info in resp.json().get('dtos'):
                            channel.update(info)
                            for businessType in [
                                {'businessType': 2, 'Type': '企业出厂价'},
                                {'businessType': 3, 'Type': '国内市场价'},
                                {'businessType': 4, 'Type': '国际市场价'},
                            ]:
                                channel.update(businessType)
                                varietiesName = quote(channel['varietiesName'])
                                varietiesId = channel['varietiesId']
                                channelId = channel['channelId']
                                oneName = quote(channel['oneName'])
                                twoName = quote(channel['twoName'])
                                templateType = channel['templateType']
                                link = f'https://dc.oilchem.net/price_search/list.htm?businessType={businessType["businessType"]}&varietiesName={varietiesName}&varietiesId={varietiesId}&{templateType}&channelId={channelId}&oneName={oneName}&twoName={twoName}'
                                channel.update({'link': link})
                                self.category_coll.update_one({'link': channel['link']}, {'$set': channel}, upsert=True)
                                print(channel)
                else:
                    logger.warning(resp.text)
            except Exception as error:
                logger.warning(error)

        print('lz_sj 获取所有类目--完成')

    """
        获取详细类目
    """

    # 加载详细产品数据
    def GetDetailCategory(self, info):
        Link = info.get('link')

        print(Link)
        self.categoryDataHeaders.update({
            'Cookie': self.cookie_coll.find_one({'name': 'lz_sj_category'}).get('cookie'),
            'referer': Link
        })

        conn = self.MySql()

        try:
            resp = requests.get(url=Link, headers=self.categoryDataHeaders, timeout=5, verify=False)
            if resp.status_code == 200:
                resp.encoding = 'utf-8'

                data = self.ParseDetailCategory(resp.text)
                if data:
                    # 判断数据 如果 请登录 超过10个 启用登陆程序
                    wrongInfo = len(re.findall("请登录", str(data), re.S))
                    if wrongInfo > 10:
                        return
                    else:
                        for item in data.items():
                            if isinstance(item[1], list):
                                for msg in item[1]:
                                    # print(msg)
                                    hashKey = hashlib.md5(str(
                                        item[0] + info.get('varietiesName') + msg.get('input').get(
                                            'businessIdList')).encode("utf8")).hexdigest()
                                    # 插入数据
                                    try:
                                        insert_data = {
                                            "hashKey": hashKey,
                                            "link": Link,
                                            "oneName": info.get('oneName'),
                                            "twoName": info.get('twoName'),
                                            "varietiesName": info.get('varietiesName'),
                                            "location": item[0],
                                            "data": msg,
                                            "time": time.strftime("%Y-%m-%d", time.localtime())
                                        }
                                        print(insert_data)
                                        self.categoryData_coll.update_one({'hashKey': hashKey}, {'$set': insert_data},
                                                                          upsert=True)
                                    except Exception as error:
                                        logger.warning(error)
                        self.category_coll.update_one({'link': info['link']}, {'$set': {'status': 1}}, upsert=True)

                        # 解析类目
                        self.ParserCategory(conn, Link, info['Type'])
                else:
                    print('没有数据 %s ' % str(resp.status_code))
                    self.category_coll.update_one({'link': info['link']}, {'$set': {'status': 400}}, upsert=True)
            else:
                logger.warning(resp.text)
        except TimeoutError:
            logger.warning(Link)
        except Exception as error:
            logger.warning(error)
        finally:
            # 关闭MySQL连接
            conn.cursor().close()

        # 随机休眠
        time.sleep(random.uniform(1, 5))

    # 解析详细产品分类数据
    @staticmethod
    def ParseDetailCategory(html):
        soup = BeautifulSoup(html, 'lxml')
        info = {}
        for style in soup.find('div', {'class': 'containerList line-height22'}).find_all('div', {
            'style': 'margin-bottom: 20px;'}):
            # 区域
            try:
                location = style.find('span', {'style': 'font-size: 16px;'}).get_text()
            except:
                location = None

            try:
                titles = [th.get_text() for th in
                          style.find('table', {'class': 'table'}).find_all('tr')[0].find_all('th')]

                dataList = []
                for tr in style.find('table', {'class': 'table'}).find_all('tr')[1:]:
                    try:
                        values = [td.get_text().replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '')
                                  for td in tr.find_all('td')]
                        if len(values) >= len(titles):
                            numsList = []
                            for n in range(len(titles) - 11):
                                numsList.append('-')
                            newValues = values[:5] + numsList + values[-6:]
                            data = {}
                            data.update({'input':
                                {
                                    "businessType":
                                        re.findall('data-business-type="(.*?)"', str(tr.find_all('td')[-1]), re.S)[0],
                                    "varietiesId":
                                        re.findall('data-varieties-id="(.*?)"', str(tr.find_all('td')[-1]), re.S)[0],
                                    "businessIdList": re.findall('value="(.*?)"', str(tr.find_all('td')[-1]), re.S)[0]
                                }
                            })

                            for num in range(len(titles)):
                                data.update({
                                    titles[num]: newValues[num]
                                })
                            dataList.append(data)
                        else:
                            pass
                    except:
                        pass
                if dataList:
                    info.update({
                        location: dataList
                    })
            except Exception as error:
                logger.warning(error)
        if info:
            # pp.pprint(info)
            return info

    # 获取产品类目数据
    def ParserCategory(self, conn, source_url, Type):
        try:
            insertSql = '''INSERT INTO lz_prod_category(hashKey, quotation_type, prod_price_type, prod_name, prod_area, prod_factory,
            prod_sales_company, prod_specifications, prod_remark, plat_source_id, create_time, update_time, plat_source_url)
            VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%d','%s','%s','%s')'''

            updateSql = "update lz_prod_category set quotation_type='%s', prod_price_type='%s', prod_name='%s', prod_area='%s', prod_factory='%s', prod_sales_company='%s', prod_specifications='%s', prod_remark='%s', plat_source_id='%d', update_time='%s', plat_source_url='%s' where hashKey='%s'"

            channelId = source_url.split('channelId=')[1].split('&')[0]
            templateType = source_url.split('templateType=')[1].split('&')[0]
            varietiesId = source_url.split('varietiesId=')[1].split('&')[0]
            varietiesName = str(unquote(source_url.split('varietiesName=')[1].split('&')[0]))
            prod_remark = ''  # 备注
            plat_source_id = 1  # 数据来源
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_url = source_url  # 数据来源网址

            if Type == '企业出厂价':
                # 选择品种
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '14',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': '*/*',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608133066,1608172567,1608187804,1608199697; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608133066,1608172567,1608187804,1608199697; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608201236; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608201237',
                    }

                    link = 'https://dc.oilchem.net/priceCompany/channellist.htm'

                    urlData = {
                        'channelId': channelId
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('dtos'):
                        prod_name = []
                        for item in resp.json().get('dtos'):
                            if item.get('varietiesName'):
                                prod_name.append(item.get('varietiesName'))
                            else:
                                pass
                    else:
                        prod_name = ''
                except:
                    prod_name = ''

                # 选择区域
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '111',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608202258; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608202258'
                    }

                    link = 'https://dc.oilchem.net/priceCompany/list_region.htm'

                    urlData = {
                        'templateType': templateType,
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'subjectionType': '',
                        'salesCompanyId': '',
                        'standard': '',
                        'priceType': '',
                        'memberId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_area = []
                        for item in resp.json().get('list'):
                            if item.get('regionName'):
                                prod_area.append(item.get('regionName'))
                            else:
                                pass
                    else:
                        prod_area = ''
                except:
                    prod_area = ''

                # 生产企业
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '111',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608208710; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608208710'
                    }

                    link = 'https://dc.oilchem.net/priceCompany/memberlist.htm'

                    urlData = {
                        'templateType': templateType,
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'subjectionType': '',
                        'regionId': '',
                        'standard': '',
                        'priceType': '',
                        'salesCompanyId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_factory = []
                        for item in resp.json().get('list'):
                            if item.get('memberAbbreviation'):
                                prod_factory.append(item.get('memberAbbreviation'))
                            else:
                                pass
                    else:
                        prod_factory = ''
                except:
                    prod_factory = ''

                # 销售公司
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '105',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608208710; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608208710',
                    }

                    link = 'https://dc.oilchem.net/priceCompany/list_sales_company.htm'

                    urlData = {
                        'templateType': templateType,
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'subjectionType': '',
                        'regionId': '',
                        'standard': '',
                        'priceType': '',
                        'memberId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_sales_company = []
                        for item in resp.json().get('list'):
                            if item.get('salesCompanyName'):
                                prod_sales_company.append(item.get('salesCompanyName'))
                            else:
                                pass
                    else:
                        prod_sales_company = ''
                except:
                    prod_sales_company = ''

                # 规格型号
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '103',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608209306; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608209306',
                    }

                    link = 'https://dc.oilchem.net/priceCompany/list_specifications.htm'

                    urlData = {
                        'templateType': templateType,
                        'varietiesId': varietiesId,
                        'subjectionType': '',
                        'salesCompanyId': '',
                        'regionId': '',
                        'standard': '',
                        'priceType': '',
                        'memberId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_specifications = []
                        for item in resp.json().get('list'):
                            if item.get('specificationsName'):
                                prod_specifications.append(item.get('specificationsName'))
                            else:
                                pass
                    else:
                        prod_specifications = ''
                except:
                    prod_specifications = ''

                # 规格类型
                prod_price_type = ''

                hashKey = hashlib.md5((str(Type + channelId + templateType + varietiesId + str(prod_name))).encode(
                    "utf8")).hexdigest()  # 数据唯一索引

            elif Type == '国内市场价':
                # 选择品种
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '14',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': '*/*',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608133066,1608172567,1608187804,1608199697; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608133066,1608172567,1608187804,1608199697; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608201236; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608201237',
                    }

                    link = 'https://dc.oilchem.net/priceDomestic/channellist.htm'

                    urlData = {
                        'channelId': channelId
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('dtos'):
                        prod_name = []
                        for item in resp.json().get('dtos'):
                            if item.get('varietiesName'):
                                prod_name.append(item.get('varietiesName'))
                            else:
                                pass
                    else:
                        prod_name = ''
                except:
                    prod_name = ''

                # 选择区域
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '111',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608202258; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608202258'
                    }

                    link = 'https://dc.oilchem.net/priceDomestic/list_region.htm'

                    urlData = {
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'memberId': '',
                        'standard': '',
                        'productState': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_area = []
                        for item in resp.json().get('list'):
                            if item.get('regionName'):
                                prod_area.append(item.get('regionName'))
                            else:
                                pass
                    else:
                        prod_area = ''
                except:
                    prod_area = ''

                # 生产企业
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '111',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608208710; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608208710'
                    }

                    link = 'https://dc.oilchem.net/priceDomestic/list_member.htm'

                    urlData = {
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'regionId': '',
                        'standard': '',
                        'productState': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_factory = []
                        for item in resp.json().get('list'):
                            if item.get('memberAbbreviation'):
                                prod_factory.append(item.get('memberAbbreviation'))
                            else:
                                pass
                    else:
                        prod_factory = ''
                except:
                    prod_factory = ''

                # 销售公司
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '105',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608208710; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608208710',
                    }

                    link = 'https://dc.oilchem.net/priceDomestic/list_sales_company.htm'

                    urlData = {
                        'templateType': 6,
                        'varietiesId': 313,
                        'specificationsId': '',
                        'subjectionType': '',
                        'regionId': '',
                        'standard': '',
                        'priceType': '',
                        'memberId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_sales_company = []
                        for item in resp.json().get('list'):
                            if item.get('salesCompanyName'):
                                prod_sales_company.append(item.get('salesCompanyName'))
                            else:
                                pass
                    else:
                        prod_sales_company = ''
                except:
                    prod_sales_company = ''

                # 规格型号
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '103',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608209306; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608209306',
                    }

                    link = 'https://dc.oilchem.net/priceDomestic/list_specifications.htm'

                    urlData = {
                        'varietiesId': varietiesId,
                        'memberId': '',
                        'regionId': '',
                        'standard': '',
                        'productState': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_specifications = []
                        for item in resp.json().get('list'):
                            if item.get('specificationsName'):
                                prod_specifications.append(item.get('specificationsName'))
                            else:
                                pass
                    else:
                        prod_specifications = ''
                except:
                    prod_specifications = ''

                # 规格类型
                prod_price_type = ''

                hashKey = hashlib.md5((str(Type + channelId + templateType + varietiesId + str(prod_name))).encode(
                    "utf8")).hexdigest()  # 数据唯一索引

            elif Type == '国际市场价':
                # 选择品种
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '14',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': '*/*',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608133066,1608172567,1608187804,1608199697; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608133066,1608172567,1608187804,1608199697; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608201236; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608201237',
                    }

                    link = 'https://dc.oilchem.net/priceInternational/channellist.htm'

                    urlData = {
                        'channelId': channelId
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('dtos'):
                        prod_name = []
                        for item in resp.json().get('dtos'):
                            if item.get('varietiesName'):
                                prod_name.append(item.get('varietiesName'))
                            else:
                                pass
                    else:
                        prod_name = ''
                except:
                    prod_name = ''

                # 选择区域
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '111',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608202258; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608202258'
                    }

                    link = 'https://dc.oilchem.net/priceInternational/list_region.htm'

                    urlData = {
                        'varietiesId': varietiesId,
                        'priceType': '',
                        'specificationsId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_area = []
                        for item in resp.json().get('list'):
                            if item.get('regionName'):
                                prod_area.append(item.get('regionName'))
                            else:
                                pass
                    else:
                        prod_area = ''
                except:
                    prod_area = ''

                # 生产企业
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '111',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608208710; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608208710'
                    }

                    link = 'https://dc.oilchem.net/priceInternational/memberlist.htm'

                    urlData = {
                        'templateType': templateType,
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'subjectionType': '',
                        'regionId': '',
                        'standard': '',
                        'priceType': '',
                        'salesCompanyId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_factory = []
                        for item in resp.json().get('list'):
                            if item.get('memberAbbreviation'):
                                prod_factory.append(item.get('memberAbbreviation'))
                            else:
                                pass
                    else:
                        prod_factory = ''
                except:
                    prod_factory = ''

                # 销售公司
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '105',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608208710; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608208710',
                    }

                    link = 'https://dc.oilchem.net/priceInternational/list_sales_company.htm'

                    urlData = {
                        'templateType': templateType,
                        'varietiesId': varietiesId,
                        'specificationsId': '',
                        'subjectionType': '',
                        'regionId': '',
                        'standard': '',
                        'priceType': '',
                        'memberId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_sales_company = []
                        for item in resp.json().get('list'):
                            if item.get('salesCompanyName'):
                                prod_sales_company.append(item.get('salesCompanyName'))
                            else:
                                pass
                    else:
                        prod_sales_company = ''
                except:
                    prod_sales_company = ''

                # 规格型号
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '103',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608209306; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608209306',
                    }

                    link = 'https://dc.oilchem.net/priceInternational/list_specifications.htm'

                    urlData = {
                        'varietiesId': varietiesId,
                        'priceType': '',
                        'regionId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_specifications = []
                        for item in resp.json().get('list'):
                            if item.get('specificationsName'):
                                prod_specifications.append(item.get('specificationsName'))
                            else:
                                pass
                    else:
                        prod_specifications = ''
                except:
                    prod_specifications = ''

                # 规格类型
                try:
                    headers = {
                        'Host': 'dc.oilchem.net',
                        'Connection': 'keep-alive',
                        'Content-Length': '103',
                        'Pragma': 'no-cache',
                        'Cache-Control': 'no-cache',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Origin': 'https://dc.oilchem.net',
                        'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cookie': '_username=13428976742; _member_user_tonken_=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzZWMiOiIkMmEkMTAkODF0R2I4UUVHL2tPNmY0RU13ZmZuLk9QQjE5TUEzbTdtV1ZnQ1pMdnBoMFprVkxUNGFYMnUiLCJuaWNrTmFtZSI6IiIsInBpYyI6IiIsImV4cCI6MTYxMDIwOTc4NCwidXNlcklkIjoxMTExNDMzLCJpYXQiOjE2MDc2MTc3ODQsImp0aSI6IjBkZjljNzU3LWU5MjUtNDA2My05YTYzLTZhYTM3ZDQ3ZWJlNyIsInVzZXJuYW1lIjoiMTM0Mjg5NzY3NDIifQ.rJ5pLdhTQ6aPP_OjxGOb-2vPa7PJGdDUzBqNOF9F5m4; refcheck=ok; refpay=0; refsite=; oilchem_refer_url=; oilchem_land_url=https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7; Hm_lvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608172567,1608187804,1608199697,1608202237; Hm_lvt_47f485baba18aaaa71d17def87b5f7ec=1608172567,1608187804,1608199697,1608202237; Hm_lpvt_e91cc445fdd1ff22a6e5c7ea9e9d5406=1608209306; Hm_lpvt_47f485baba18aaaa71d17def87b5f7ec=1608209306',
                    }

                    link = 'https://dc.oilchem.net/priceInternational/list_pricestandard.htm'

                    urlData = {
                        'varietiesId': varietiesId,
                        'regionId': '',
                        'specificationsId': ''
                    }

                    urlData = urlencode(urlData)

                    resp = requests.post(link, headers=headers, data=urlData, verify=False)
                    if resp.json() and resp.json().get('list'):
                        prod_price_type = []
                        for item in resp.json().get('list'):
                            if item.get('priceTypeName'):
                                prod_price_type.append(item.get('priceTypeName'))
                            else:
                                pass
                        if prod_price_type:
                            prod_price_type = str(prod_price_type).replace("'", '"')
                        else:
                            prod_price_type = ''
                    else:
                        prod_price_type = ''
                except:
                    prod_price_type = ''

                hashKey = hashlib.md5((str(Type + channelId + templateType + varietiesId + str(prod_name))).encode(
                    "utf8")).hexdigest()  # 数据唯一索引

            else:
                return

            insertData = (
                hashKey,
                Type,
                prod_price_type if prod_price_type else '',
                varietiesName,
                str(prod_area).replace("'", '"') if prod_area else '',
                str(prod_factory).replace("'", '"') if prod_factory else '',
                str(prod_sales_company).replace("'", '"') if prod_sales_company else '',
                str(prod_specifications).replace("'", '"') if prod_specifications else '',
                prod_remark,
                plat_source_id,
                create_time,
                update_time,
                plat_source_url
            )

            updateData = (
                Type,
                prod_price_type if prod_price_type else '',
                varietiesName,
                str(prod_area).replace("'", '"') if prod_area else '',
                str(prod_factory).replace("'", '"') if prod_factory else '',
                str(prod_sales_company).replace("'", '"') if prod_sales_company else '',
                str(prod_specifications).replace("'", '"') if prod_specifications else '',
                prod_remark,
                plat_source_id,
                update_time,
                plat_source_url,
                hashKey
            )

            if prod_price_type or prod_area or prod_factory or prod_sales_company or prod_specifications:
                self.UpdateToMysql(conn, insertSql, insertData, updateSql, updateData)
        except:
            print('错误在这里')

    """
        获取历史数据
    """

    # 下载历史数据
    def DownloadHistoryData(self, info, proxy=False, history=False):
        print(info)
        hash_key = info.get('hashKey')
        TypeInfo = {'2': '企业出厂价', '3': '国内市场价', '4': '国际市场价'}

        try:
            downloadTime = str(time.strftime("%Y-%m-%d", time.localtime(time.time()))).replace('-', '')
            businessType = info.get('data').get('input').get('businessType')
            Type = TypeInfo[businessType]
            varietiesId = info.get('data').get('input').get('varietiesId')
            businessIdList = info.get('data').get('input').get('businessIdList')
            productFormat = info.get('data').get('标准')

            url = 'https://dc.oilchem.net/price_search/doExportZip.htm?startDate=20190101&endDate={0}' \
                  '&indexType=0&businessInformation=%5B%7B%22businessType%22%3A%22{1}%22%2C%22varietiesId%22%3A%22{2}%22%2C%22businessIdList%22%3A%5B{3}%5D%7D%5D' \
                  '&fileName={4}.xlsx'.format(downloadTime, businessType, varietiesId, businessIdList, hash_key)
            print(url)

            self.downloadDetailHeaders.update({
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                'Connection': 'keep-alive',
                'Cookie': self.cookie_coll.find_one({'name': 'lz_sj_downloadDetail'}).get('cookie'),
                'Host': 'dc.oilchem.net',
                'Referer': 'https://dc.oilchem.net/price_search/detail.htm?channelId=1777&varietiesId=317&id=8287&timeType=0&flag=0&businessType=3&indexPriceType=2',
                'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Microsoft Edge";v="100"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36 Edg/100.0.1185.44'
            })

            resp = requests.get(url, headers=self.downloadDetailHeaders, timeout=5, verify=False)

            if resp.content:
                _fh = self.downloadPath + r'/lz/{}'.format(businessType)
                if not os.path.exists(_fh):
                    os.makedirs(_fh)

                fp = _fh + '/{}.xlsx'.format(hash_key)
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()

                # 读取 excel 表格数据
                self.GetDataFromExcel(Type, productFormat, url, fp, hash_key, history)
                self.categoryData_coll.update_one({'hashKey': info['hashKey']}, {'$set': {'status': 1}}, upsert=True)
            else:
                print(f'--- Cookie过期或者页面失效 hash_key: {info["hashKey"]}---')
                self.categoryData_coll.update_one({'hashKey': info['hashKey']}, {'$set': {'status': 404}}, upsert=True)
        except requests.exceptions.ConnectionError:
            return self.DownloadHistoryData(info, proxy, history)
        except TimeoutError:
            logger.warning(info)
        except Exception as error:
            logger.warning(error)

        # 随机休眠
        if not history:
            time.sleep(random.uniform(1, 1.5))

    # 读取 excel 表格数据
    def GetDataFromExcel(self, businessType, productFormat, sourceLink, fp, hash_key, history):
        conn = self.MySql()

        try:
            dataFrame = pd.read_excel(fp, header=None)
            detailData = dataFrame.to_dict(orient='index')

            if not detailData:
                return
            else:
                dumpsData = json.dumps(detailData)
                keyList = list(list(json.loads(dumpsData).values())[0].values())

                if history:
                    for value in list(json.loads(dumpsData).values())[1:]:
                        self.FormatData(conn, businessType, productFormat, sourceLink, hash_key, keyList, value)
                else:
                    for value in list(json.loads(dumpsData).values())[1:8]:
                        self.FormatData(conn, businessType, productFormat, sourceLink, hash_key, keyList, value)
        except Exception as error:
            logger.warning('该链接数据报错 {0}  {1}'.format(sourceLink, error))
            pass
        finally:
            # 关闭MySQL连接
            conn.cursor().close()

    # 格式化数据
    def FormatData(self, conn, businessType, productFormat, link, hash_key, keyList, data):
        # 企业出厂价
        if businessType == '企业出厂价':
            try:
                dt = str(data.get(str(keyList.index('日期')))).replace('/', '').replace('nan', '').replace('-',
                                                                                                         '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null',
                                                                                '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            except ValueError:
                dt = ''
            except Exception as error:
                logger.warning(error)
                dt = ''
            dt_type = 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_standard = str(productFormat).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                               '').replace(
                'Null', '').replace('null', '')  # 产品标准

            try:
                prod_name = str(data.get(str(keyList.index('产品名')))).replace('/', '').replace('nan', '').replace('-',
                                                                                                                 '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 产品名称
            except ValueError:
                prod_name = ''
            except Exception as error:
                prod_name = ''
                logger.warning(error)

            try:
                prod_specifications = str(data.get(str(keyList.index('规格型号')))).replace('/', '').replace('nan',
                                                                                                         '').replace(
                    '-', '').replace('None', '').replace('none', '').replace('Null', '').replace('null', '').replace(
                    '.0', '')  # 产品规格
            except ValueError:
                prod_specifications = ''
            except Exception as error:
                prod_specifications = ''
                logger.warning(error)

            try:
                prod_area = str(data.get(str(keyList.index('所在区域')))).replace('/', '').replace('nan', '').replace('-',
                                                                                                                  '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 产品区域
            except ValueError:
                prod_area = ''
            except Exception as error:
                prod_area = ''
                logger.warning(error)

            try:
                prod_factory = str(data.get(str(keyList.index('企业名称')))).replace('/', '').replace('nan', '').replace(
                    '-', '').replace('None', '').replace('none', '').replace('Null', '').replace('null', '')  # 生产企业
            except ValueError:
                prod_factory = ''
            except Exception as error:
                prod_factory = ''
                logger.warning(error)

            try:
                prod_sales_company = str(data.get(str(keyList.index('销售公司')))).replace('/', '').replace('nan',
                                                                                                        '').replace('-',
                                                                                                                    '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 销售公司(默认为空,隆众有这字段)
            except ValueError:
                prod_sales_company = ''
            except Exception as error:
                prod_sales_company = ''
                logger.warning(error)

            prod_lowest_price = 0.00  # 最低价
            prod_highest_price = 0.00  # 最高价

            try:
                prod_average_price = str(data.get(str(keyList.index('报价')))).replace('/', '').replace('nan',
                                                                                                      '').replace('-',
                                                                                                                  '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 平均价(报价)
                if prod_average_price:
                    prod_average_price = round(float(prod_average_price), 2)
            except ValueError:
                prod_average_price = 0.00
            except Exception as error:
                prod_average_price = 0.00
                logger.warning(error)

            try:
                prod_unit = str(data.get(str(keyList.index('单位')))).replace('/', '').replace('nan', '').replace('-',
                                                                                                                '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 单位
            except ValueError:
                prod_unit = ''
            except Exception as error:
                prod_unit = ''
                logger.warning(error)

            try:
                prod_change_amount = str(data.get(str(keyList.index('涨跌幅')))).replace('/', '').replace('nan',
                                                                                                       '').replace('-',
                                                                                                                   '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 涨跌额
                if prod_change_amount:
                    try:
                        prod_change_amount = round(float(prod_change_amount), 4)
                    except:
                        prod_change_amount = 0.00
                else:
                    prod_change_amount = 0.00
            except ValueError:
                prod_change_amount = ''
            except Exception as error:
                prod_change_amount = ''
                logger.warning(error)

            try:
                prod_change_rate = str(data.get(str(keyList.index('涨跌率')))).replace('/', '').replace('nan', '').replace(
                    '-', '').replace('None', '').replace('none', '').replace('Null', '').replace('null', '')  # 涨跌率
                if prod_change_rate:
                    try:
                        prod_change_rate = round(float(prod_change_rate), 4)
                    except:
                        prod_change_rate = 0.00
                else:
                    prod_change_rate = 0.00
            except ValueError:
                prod_change_rate = ''
            except Exception as error:
                prod_change_rate = ''
                logger.warning(error)

            price_conditions = ''  # 价格条件(默认为空)

            try:
                prod_remark = str(data.get(str(keyList.index('备注')))).replace('/', '').replace('nan', '').replace('-',
                                                                                                                  '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '')  # 备注
            except ValueError:
                prod_remark = ''
            except Exception as error:
                prod_remark = ''
                logger.warning(error)

            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 1  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = businessType  # 数据来源备注
            plat_source_url = link  # 数据来源网址

            hashKey = hashlib.md5((hash_key + str(dt)).encode("utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO lz_factory_produce_price(hashKey, dt, dt_type, prod_name, prod_standard, prod_specifications,
            prod_area, prod_factory, prod_sales_company, prod_lowest_price, prod_highest_price, prod_average_price, prod_unit,
            prod_change_amount, prod_change_rate, price_conditions, prod_remark, create_time, update_time, plat_source_id,
            plat_source_remark, plat_source_url)
            VALUES('%s','%s','%d','%s','%s','%s',
            '%s','%s','%s','%f','%f','%f','%s',
            '%f','%f','%s','%s','%s','%s','%d',
            '%s','%s')'''

            insertData = (
                hashKey,
                dt if dt else '',
                dt_type,
                prod_name if prod_name else '',
                prod_standard if prod_standard else '',
                prod_specifications if prod_specifications else '',
                prod_area if prod_area else '',
                prod_factory if prod_factory else '',
                prod_sales_company if prod_sales_company else '',
                prod_lowest_price if prod_lowest_price else 0.00,
                prod_highest_price if prod_highest_price else 0.00,
                prod_average_price if prod_average_price else 0.00,
                prod_unit if prod_unit else '',
                prod_change_amount if prod_change_amount else 0.00,
                prod_change_rate if prod_change_rate else 0.00,
                price_conditions if price_conditions else '',
                prod_remark if prod_remark else '',
                create_time,
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url
            )
            # print(insertData)

            updateSql = "update lz_factory_produce_price set dt='%s', dt_type='%d', prod_name='%s', prod_standard='%s', prod_specifications='%s'," \
                        "prod_area='%s', prod_factory='%s', prod_sales_company='%s', prod_lowest_price='%f', prod_highest_price='%f', prod_average_price='%f', prod_unit='%s'," \
                        "prod_change_amount='%f', prod_change_rate='%f', price_conditions='%s', prod_remark='%s', update_time='%s', plat_source_id='%d'," \
                        "plat_source_remark='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                dt if dt else '',
                dt_type,
                prod_name if prod_name else '',
                prod_standard if prod_standard else '',
                prod_specifications if prod_specifications else '',
                prod_area if prod_area else '',
                prod_factory if prod_factory else '',
                prod_sales_company if prod_sales_company else '',
                prod_lowest_price if prod_lowest_price else 0.00,
                prod_highest_price if prod_highest_price else 0.00,
                prod_average_price if prod_average_price else 0.00,
                prod_unit if prod_unit else '',
                prod_change_amount if prod_change_amount else 0.00,
                prod_change_rate if prod_change_rate else 0.00,
                price_conditions if price_conditions else '',
                prod_remark if prod_remark else '',
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url,
                hashKey
            )
            # print(updateData)

        # 国内市场价
        elif businessType == '国内市场价':
            try:
                dt = str(data.get(str(keyList.index('日期')))).replace('/',
                                                                     '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            except ValueError:
                dt = ''
            except Exception as error:
                dt = ''
                logger.warning(error)
            dt_type = 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_name = str(data.get('1')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                           '').replace(
                'Null', '').replace('null', '')  # 产品名称
            prod_area = str(data.get('4')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                           '').replace(
                'Null', '').replace('null', '')  # 产品区域
            prod_factory = str(data.get('6')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                              '').replace(
                'Null', '').replace('null', '')  # 生产企业
            prod_market = str(data.get('5')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                             '').replace(
                'Null', '').replace('null', '')  # 产品市场
            prod_standard = str(data.get('3')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                               '').replace(
                'Null', '').replace('null', '')  # 产品标准
            prod_specifications = str(data.get('2')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '').replace('.0', '')  # 产品规格
            price_conditions = ''  # 价格条件(默认为空)
            prod_lowest_price = str(data.get('7')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '')  # 最低价
            if prod_lowest_price:
                prod_lowest_price = round(float(prod_lowest_price), 2)
            prod_highest_price = str(data.get('8')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '')  # 最高价
            if prod_highest_price:
                prod_highest_price = round(float(prod_highest_price), 2)
            prod_average_price = str(data.get('9')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '')  # 平均价(报价)
            if prod_average_price:
                prod_average_price = round(float(prod_average_price), 2)
            prod_change_amount = str(data.get('11')).replace('nan', '').replace('None', '').replace('none', '').replace(
                'Null', '').replace('null', '')  # 涨跌额
            if prod_change_amount:
                try:
                    prod_change_amount = round(float(prod_change_amount), 4)
                except:
                    prod_change_amount = 0.00
            prod_change_rate = str(data.get('12')).replace('nan', '').replace('None', '').replace('none', '').replace(
                'Null', '').replace('null', '')  # 涨跌率
            if prod_change_rate:
                try:
                    prod_change_rate = round(float(prod_change_rate), 6)
                except:
                    prod_change_rate = 0.00
            prod_unit = data.get('10')  # 单位
            prod_remark = str(data.get('13')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                              '').replace(
                'Null', '').replace('null', '')  # 备注
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 1  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = businessType  # 数据来源备注
            plat_source_url = link  # 数据来源网址

            hashKey = hashlib.md5((hash_key + str(dt)).encode("utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO lz_domestic_market_price(hashKey, dt, dt_type, prod_name, prod_area, prod_factory,
            prod_market, prod_standard, prod_specifications, price_conditions, prod_lowest_price, prod_highest_price, prod_average_price,
            prod_change_amount, prod_change_rate, prod_unit, prod_remark, create_time, update_time, plat_source_id,
            plat_source_remark, plat_source_url)
            VALUES('%s','%s','%d','%s','%s','%s',
            '%s','%s','%s','%s','%f','%f','%f',
            '%f','%f','%s','%s','%s','%s','%d',
            '%s','%s')'''

            insertData = (
                hashKey,
                dt if dt else '',
                dt_type,
                prod_name if prod_name else '',
                prod_area if prod_area else '',
                prod_factory if prod_factory else '',
                prod_market if prod_market else '',
                prod_standard if prod_standard else '',
                prod_specifications if prod_specifications else '',
                price_conditions if price_conditions else '',
                prod_lowest_price if prod_lowest_price else 0.00,
                prod_highest_price if prod_highest_price else 0.00,
                prod_average_price if prod_average_price else 0.00,
                prod_change_amount if prod_change_amount else 0.00,
                prod_change_rate if prod_change_rate else 0.00,
                prod_unit if prod_unit else '',
                prod_remark if prod_remark else '',
                create_time,
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url
            )
            # print(insertData)

            updateSql = "update lz_domestic_market_price set dt='%s', dt_type='%d', prod_name='%s', prod_area='%s', prod_factory='%s'," \
                        "prod_market='%s', prod_standard='%s', prod_specifications='%s', price_conditions='%s', prod_lowest_price='%f', prod_highest_price='%f', prod_average_price='%f'," \
                        "prod_change_amount='%f', prod_change_rate='%f', prod_unit='%s', prod_remark='%s', update_time='%s', plat_source_id='%d'," \
                        "plat_source_remark='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                dt if dt else '',
                dt_type,
                prod_name if prod_name else '',
                prod_area if prod_area else '',
                prod_factory if prod_factory else '',
                prod_market if prod_market else '',
                prod_standard if prod_standard else '',
                prod_specifications if prod_specifications else '',
                price_conditions if price_conditions else '',
                prod_lowest_price if prod_lowest_price else 0.00,
                prod_highest_price if prod_highest_price else 0.00,
                prod_average_price if prod_average_price else 0.00,
                prod_change_amount if prod_change_amount else 0.00,
                prod_change_rate if prod_change_rate else 0.00,
                prod_unit if prod_unit else '',
                prod_remark if prod_remark else '',
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url,
                hashKey
            )
            # print(updateData)

        # 国际市场价
        elif businessType == '国际市场价':
            try:
                dt = str(data.get(str(keyList.index('日期')))).replace('/',
                                                                     '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            except ValueError:
                dt = ''
            except Exception as error:
                dt = ''
                logger.warning(error)
            dt_type = 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_name = str(data.get('1')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                           '').replace(
                'Null', '').replace('null', '')  # 产品名称
            prod_area = str(data.get('3')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                           '').replace(
                'Null', '').replace('null', '')  # 产品区域
            prod_factory = ''  # 生产企业
            prod_market = ''  # 产品市场
            prod_price_type = str(data.get('4')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                                 '').replace(
                'Null', '').replace('null', '')  # 价格类型
            prod_specifications = str(data.get('2')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '').replace('.0', '')  # 产品规格
            prod_lowest_price = str(data.get('5')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '')  # 最低价
            if prod_lowest_price:
                prod_lowest_price = round(float(prod_lowest_price), 2)
            prod_highest_price = str(data.get('6')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '')  # 最高价
            if prod_highest_price:
                prod_highest_price = round(float(prod_highest_price), 2)
            prod_average_price = str(data.get('7')).replace('nan', '').replace('-', '').replace('None', '').replace(
                'none', '').replace('Null', '').replace('null', '')  # 平均价(报价)
            if prod_average_price:
                prod_average_price = round(float(prod_average_price), 2)
            prod_change_amount = str(data.get('9')).replace('nan', '').replace('None', '').replace('none', '').replace(
                'Null', '').replace('null', '')  # 涨跌额
            if prod_change_amount:
                try:
                    prod_change_amount = round(float(prod_change_amount), 4)
                except:
                    prod_change_amount = 0.00
            prod_change_rate = str(data.get('10')).replace('nan', '').replace('None', '').replace('none', '').replace(
                'Null', '').replace('null', '')  # 涨跌率
            if prod_change_rate:
                try:
                    prod_change_rate = round(float(prod_change_rate), 6)
                except:
                    prod_change_rate = 0.00
            prod_unit = data.get('8')  # 单位
            price_conditions = ''  # 价格条件(卓创, 默认为空)
            lz_rmb_price = str(data.get('11')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                               '').replace(
                'Null', '').replace('null', '')  # 隆众人民币价格
            prod_remark = str(data.get('12')).replace('nan', '').replace('-', '').replace('None', '').replace('none',
                                                                                                              '').replace(
                'Null', '').replace('null', '')  # 备注
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 1  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = businessType  # 数据来源备注
            plat_source_url = link  # 数据来源网址

            hashKey = hashlib.md5((hash_key + str(dt)).encode("utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO lz_international_market_price(hashKey, dt, dt_type, prod_name, prod_area, prod_factory,
            prod_market, prod_price_type, prod_specifications, prod_lowest_price, prod_highest_price, prod_average_price,
            prod_change_amount, prod_change_rate, prod_unit, price_conditions, lz_rmb_price, prod_remark, create_time,
            update_time, plat_source_id, plat_source_remark, plat_source_url)
            VALUES('%s','%s','%d','%s','%s','%s',
            '%s','%s','%s','%f','%f','%f',
            '%f','%f','%s','%s','%s','%s','%s',
            '%s','%d','%s','%s')'''

            insertData = (
                hashKey,
                dt if dt else '',
                dt_type,
                prod_name if prod_name else '',
                prod_area if prod_area else '',
                prod_factory if prod_factory else '',
                prod_market if prod_market else '',
                prod_price_type if prod_price_type else '',
                prod_specifications if prod_specifications else '',
                prod_lowest_price if prod_lowest_price else 0.00,
                prod_highest_price if prod_highest_price else 0.00,
                prod_average_price if prod_average_price else 0.00,
                prod_change_amount if prod_change_amount else 0.00,
                prod_change_rate if prod_change_rate else 0.00,
                prod_unit if prod_unit else '',
                price_conditions if price_conditions else '',
                lz_rmb_price if lz_rmb_price else '',
                prod_remark if prod_remark else '',
                create_time,
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url
            )
            # print(insertData)

            updateSql = "update lz_international_market_price set dt='%s', dt_type='%d', prod_name='%s', prod_area='%s', prod_factory='%s'," \
                        "prod_market='%s', prod_price_type='%s', prod_specifications='%s', prod_lowest_price='%f', prod_highest_price='%f', prod_average_price='%f'," \
                        "prod_change_amount='%f', prod_change_rate='%f', prod_unit='%s', price_conditions='%s', lz_rmb_price='%s', prod_remark='%s'," \
                        "update_time='%s', plat_source_id='%d', plat_source_remark='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                dt if dt else '',
                dt_type,
                prod_name if prod_name else '',
                prod_area if prod_area else '',
                prod_factory if prod_factory else '',
                prod_market if prod_market else '',
                prod_price_type if prod_price_type else '',
                prod_specifications if prod_specifications else '',
                prod_lowest_price if prod_lowest_price else 0.00,
                prod_highest_price if prod_highest_price else 0.00,
                prod_average_price if prod_average_price else 0.00,
                prod_change_amount if prod_change_amount else 0.00,
                prod_change_rate if prod_change_rate else 0.00,
                prod_unit if prod_unit else '',
                price_conditions if price_conditions else '',
                lz_rmb_price if lz_rmb_price else '',
                prod_remark if prod_remark else '',
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url,
                hashKey
            )
            # print(updateData)

        else:
            return

        if insertData and updateData:
            self.UpdateToMysql(conn, insertSql, insertData, updateSql, updateData)

    # 更新数据到MySQL
    @staticmethod
    def UpdateToMysql(conn, insertSql, insertData, updateSql, updateData):
        cursor = conn.cursor()

        try:
            cursor.execute(insertSql % insertData)
            conn.commit()
            print(insertData)
        except IntegrityError:
            try:
                cursor.execute(updateSql % updateData)
                conn.commit()
                print(updateData)
            except Exception as error:
                logger.warning(error)
                logger.warning(updateData)
                conn.commit()

    # 还原状态
    @staticmethod
    def removeStatus(coll, hashkey):
        for num, info in enumerate(coll.find({'$nor': [{'status': None}, {'status': 400}, {'status': 404}]})):
            print(num)
            coll.update_one({hashkey: info[hashkey]}, {'$unset': {'status': ''}}, upsert=True)

    # 多线程获取数据
    def CommandThread(self, proxy=False, history=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=2)

        # 每周一更新详细类目  主类目：108  详细数据：3750
        if (pd.to_datetime(str(time.strftime("%Y-%m-%d", time.localtime(time.time())))) - pd.to_datetime(
                '20160103')).days % 7 == 1:
            # 获取主类目
            self.GetMainCategory()
            # 获取主类目下详细数据
            for category in [i for i in self.category_coll.find({'status': 400})]:
                if Async:
                    out = pool.apply_async(func=self.GetDetailCategory, args=(category,))  # 异步
                else:
                    out = pool.apply(func=self.GetDetailCategory, args=(category,))  # 同步
                thread_list.append(out)

        # 下载详细分类： 3750
        for info in self.categoryData_coll.find({'status': None}):
            if Async:
                out = pool.apply_async(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 异步
            else:
                out = pool.apply(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 同步
            thread_list.append(out)
            # break

        pool.close()
        pool.join()


def lzrun():
    lz = LongZhong()

    # 清除标记
    # if str(time.strftime("%H", time.localtime(time.time()))) == '16':
    lz.removeStatus(lz.category_coll, 'link')
    lz.removeStatus(lz.categoryData_coll, 'hashKey')

    # 多进程获取数据  params: history
    lz.CommandThread()

    print('lz_sj 获取历史数据--完成')


if __name__ == '__main__':
    lzrun()
