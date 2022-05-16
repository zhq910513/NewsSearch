#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import sys

sys.path.append("../")
import configparser
import logging
import os
import pprint
import random
import time
import json
from os import path
from typing import Dict
from urllib.parse import quote

import hashlib
import pandas as pd
import requests
from multiprocessing.pool import ThreadPool
from fake_useragent import UserAgent
from pymongo import MongoClient
import pymysql
from pymysql.err import IntegrityError

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/zhuochuang_sj.log'))
settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))
UsrPath = os.path.abspath(os.path.abspath(os.path.join(dh + '/Cookies/cookie.json')))

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


class ZhuoChuang:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")

        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=datadb))

        cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        self.category_coll = client[datadb]['zc_sj_category']
        self.categoryData_coll = client[datadb]['zc_sj_categoryData']

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

        # 请求头信息
        self.userAgent = UserAgent().random
        self.categoryUrl = 'https://prices.sci99.com/api/nav/1'
        self.categoryHeaders = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'prices.sci99.com',
            'Pragma': 'no-cache',
            'Referer': 'https://prices.sci99.com/cn/',
            'X-Requested-With': 'XMLHttpRequest',
            'user-agent': self.userAgent
        }
        self.categoryDataUrl = 'https://prices.sci99.com/api/zh-cn/product/datavalue'
        self.categoryDataHeaders = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'prices.sci99.com',
            'Origin': 'https://prices.sci99.com',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }
        self.detialDownloadUrl = 'https://prices.sci99.com/cn/product_price.aspx?diid={0}&datatypeid={1}&ppid={2}&ppname={3}&cycletype=day'
        self.defaultDownloadUrl = 'https://prices.sci99.com/api/zh-cn/dataitem/datavalue'
        self.defaultDownloadHeaders = {
            'Host': 'prices.sci99.com',
            'Connection': 'keep-alive',
            'Content-Length': '104',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Accept': '*/*',
            'Origin': 'https://prices.sci99.com',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'Content-Type': 'application/json',
            'Referer': 'https://prices.sci99.com/cn/product_price.aspx?diid=39246&datatypeid=37&ppid=12278&ppname=ldpe&cycletype=day',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
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

    # 获取分类列表
    def GetCategory(self):
        self.categoryHeaders.update({
            'Cookie': self.cookie_coll.find_one({'name': 'zc_sj_category'}).get('cookie')
        })
        try:
            resp = requests.get(self.categoryUrl, headers=self.categoryHeaders, timeout=5, verify=False)
            if resp.status_code == 200:
                respData = [s for s in resp.json().get('data') if s.get('Name') == '塑料'][0]
                for children in respData.get('Children'):
                    categoryOne = children.get('Name')
                    for child in children.get('Children'):
                        categoryTwo = child.get('Name')
                        for ch in child.get('Children'):
                            ch.update({
                                'categoryOne': categoryOne,
                                'categoryTwo': categoryTwo,
                                'categoryThree': ch.get('Name'),
                                'link': ch.get('Url').replace('http://pricev3.test.sci99.com/cn',
                                                              'https://prices.sci99.com/cn')
                            })
                            del ch['Name']
                            del ch['Children']

                            try:
                                self.category_coll.update_one({'link': ch['link']}, {'$set': ch}, upsert=True)
                            except Exception as error:
                                logger.warning(error)
                print('zc_sj 获取分类列表--完成')
            else:
                logger.warning(resp.status_code)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)

        print('zc_sj 获取分类列表--完成')

    """
        获取详细类目
    """

    # 获取当周数据/详细产品分类
    def GetDetailCategory(self, info):
        try:
            link = info.get('link')
            if info.get('categoryFour'):
                name = info.get('categoryFour')
            elif info.get('categoryThree'):
                name = info.get('categoryThree')
            elif info.get('categoryTwo'):
                name = info.get('categoryTwo')
            else:
                name = info.get('categoryOne')

            conn = self.MySql()

            for pricetypeid in [34320, 34319, 34318]:
                # 市场价格/企业报价/国际价格
                if pricetypeid == 34320:
                    Type = '市场价格'
                elif pricetypeid == 34319:
                    Type = '企业报价'
                elif pricetypeid == 34318:
                    Type = '国际价格'
                else:
                    Type = None

                self.categoryDataHeaders.update({
                    'Referer': link
                })

                jsonData = {
                    'cycletype': "day",
                    'factory': "",
                    'market': "",
                    'model': "",
                    'navid': str(info.get('ID')),
                    'pageno': 1,
                    'pagesize': 300,
                    'pname': "",
                    'ppids': str(info.get('Ppid')),
                    'ppname': str(info.get('categoryThree')),
                    'pricecycle': "",
                    'pricetypeid': pricetypeid,
                    'province': "",
                    'purpose': "",
                    'region': "",
                    'sitetype': 1,
                    'specialpricetype': ""
                }

                try:
                    if '再生' in info.get('categoryOne'):
                        self.categoryDataHeaders.update({
                            'Cookie': self.cookie_coll.find_one({'name': 'zc_sj_category_second'}).get('cookie')
                        })
                    else:
                        self.categoryDataHeaders.update({
                            'Cookie': self.cookie_coll.find_one({'name': 'zc_sj_category'}).get('cookie')
                        })
                    resp = requests.post(url=self.categoryDataUrl, headers=self.categoryDataHeaders, json=jsonData,
                                         timeout=5, verify=False)
                    if resp.status_code == 200:
                        dataJson = resp.json().get('data')
                        headers = dataJson.get('headers')

                        for item in dataJson.get('data').get('Items'):
                            hashKey = hashlib.md5(
                                str(str(info.get('ID')) + str(info.get('Ppid')) + Type + item.get('DataName')).encode(
                                    "utf8")).hexdigest()
                            itemData = {
                                'hashKey': hashKey,
                                'DIID': item.get('DIID'),
                                'fromUrl': info.get('link'),
                                'headers': headers,
                                'data': item,
                                'type': Type,
                                'time': time.strftime("%Y-%m-%d", time.localtime())
                            }

                            # 写入数据
                            self.categoryData_coll.update_one({'hashKey': hashKey}, {'$set': itemData}, upsert=True)

                        # 标记已用数据
                        self.category_coll.update_one({'link': link}, {'$set': {'status': 1}}, upsert=True)

                        # 获取解析类目
                        self.ParserCategory(conn, name, link, Type)
                    else:
                        pp.pprint(resp.text)
                except Exception as error:
                    logger.warning(error)
                    return None

                # 随机休眠
                time.sleep(random.uniform(1, 5))

            # 关闭MySQL连接
            conn.cursor().close()
        except TimeoutError:
            logger.warning(info.get('link'))
        except Exception as error:
            logger.warning(error)

    # 获取类目消息
    def ParserCategory(self, conn, prod_name, source_url, Type):
        insertSql = '''INSERT INTO zc_prod_category(hashKey, prod_name, quotation_type, prod_area, prod_market, prod_factory,
        prod_specifications, prod_standard, prod_remark, plat_source_id, create_time, update_time, plat_source_url)
        VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%d','%s','%s','%s')'''

        updateSql = "update zc_prod_category set prod_name='%s', quotation_type='%s', prod_area='%s', prod_market='%s', prod_factory='%s', " \
                    "prod_specifications='%s', prod_standard='%s', prod_remark='%s', plat_source_id='%d', update_time='%s', plat_source_url='%s' where hashKey='%s'"

        ppid = source_url.split('ppid=')[1].split('&')[0]
        navid = source_url.split('navid=')[1].split('&')[0]
        prod_remark = ''  # 备注
        plat_source_id = 2  # 数据来源
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
        plat_source_url = source_url  # 数据来源网址

        if Type == '市场价格':
            pricetypeid = 34320
            try:
                headers = {
                    'Host': 'prices.sci99.com',
                    'Connection': 'keep-alive',
                    'Content-Length': '1842',
                    'Pragma': 'no-cache',
                    'Cache-Control': 'no-cache',
                    'Accept': '*/*',
                    'Origin': 'https://prices.sci99.com',
                    'X-Requested-With': 'XMLHttpRequest',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                    'Content-Type': 'application/json',
                    'Referer': 'https://prices.sci99.com/cn/product.aspx?ppid=12278&ppname=LDPE&navid=521',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Cookie': 'guid=87ffc1de-3585-c12a-dbed-616c9c4f2d5b; UM_distinctid=17587ca66304c-0f0d4668921dcc-7a1b34-ffc00-17587ca6631b4a; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1606642739,1606676648,1606732555,1607265741; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=llblgsw1t0ktxx5ky1cmycv5; Hm_lvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608090602,1608090609,1608201146,1608265595; STATReferrerIndexId=1; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fprices.sci99.com%2Fcn%2Fproduct.aspx%3Fppid%3D12278%26ppname%3DLDPE%26navid%3D521; pageViewNum=3; Hm_lpvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608265681',
                }

                link = 'https://prices.sci99.com/api/zh-cn/dataitem/dices'

                jsonData = json.dumps([
                    {"dictype": "1", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "2", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "3", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "4", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "5", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "6", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "8", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""}])

                resp = requests.post(link, headers=headers, data=jsonData, verify=False)
                if resp.json().get('data'):
                    # 区域
                    prod_area = []
                    if isinstance(resp.json().get('data').get('1'), list):
                        for item in resp.json().get('data').get('1'):
                            prod_area.append(item.get('Name'))
                        if prod_area:
                            prod_area = str(prod_area).replace("'", '"')
                        else:
                            prod_area = ''
                    else:
                        prod_area = ''

                    # 市场
                    prod_market = []
                    if isinstance(resp.json().get('data').get('2'), list):
                        for item in resp.json().get('data').get('2'):
                            prod_market.append(item.get('Name'))
                        if prod_market:
                            prod_market = str(prod_market).replace("'", '"')
                        else:
                            prod_market = ''
                    else:
                        prod_market = ''

                    # 生产企业
                    prod_factory = []
                    if isinstance(resp.json().get('data').get('3'), list):
                        for item in resp.json().get('data').get('3'):
                            prod_factory.append(item.get('Name'))
                        if prod_factory:
                            prod_factory = str(prod_factory).replace("'", '"')
                        else:
                            prod_factory = ''
                    else:
                        prod_factory = ''

                    # 规格型号
                    prod_specifications = []
                    if isinstance(resp.json().get('data').get('5'), list):
                        for item in resp.json().get('data').get('5'):
                            prod_specifications.append(item.get('Name'))
                        if prod_specifications:
                            prod_specifications = str(prod_specifications).replace("'", '"')
                        else:
                            prod_specifications = ''
                    else:
                        prod_specifications = ''

                    # 用途分类(产品标准)
                    prod_standard = []
                    if isinstance(resp.json().get('data').get('8'), list):
                        for item in resp.json().get('data').get('8'):
                            prod_standard.append(item.get('Name'))
                        if prod_standard:
                            prod_standard = str(prod_standard).replace("'", '"')
                        else:
                            prod_standard = ''
                    else:
                        prod_standard = ''
            except:
                pass

            hashKey = hashlib.md5((str(Type + prod_name + source_url)).encode("utf8")).hexdigest()  # 数据唯一索引

        elif Type == '企业报价':
            pricetypeid = 34319
            try:
                headers = {
                    'Host': 'prices.sci99.com',
                    'Connection': 'keep-alive',
                    'Content-Length': '1842',
                    'Pragma': 'no-cache',
                    'Cache-Control': 'no-cache',
                    'Accept': '*/*',
                    'Origin': 'https://prices.sci99.com',
                    'X-Requested-With': 'XMLHttpRequest',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                    'Content-Type': 'application/json',
                    'Referer': 'https://prices.sci99.com/cn/product.aspx?ppid=12278&ppname=LDPE&navid=521',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Cookie': 'guid=87ffc1de-3585-c12a-dbed-616c9c4f2d5b; UM_distinctid=17587ca66304c-0f0d4668921dcc-7a1b34-ffc00-17587ca6631b4a; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1606642739,1606676648,1606732555,1607265741; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=llblgsw1t0ktxx5ky1cmycv5; Hm_lvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608090602,1608090609,1608201146,1608265595; STATReferrerIndexId=1; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fprices.sci99.com%2Fcn%2Fproduct.aspx%3Fppid%3D12278%26ppname%3DLDPE%26navid%3D521; pageViewNum=3; Hm_lpvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608265681',
                }

                link = 'https://prices.sci99.com/api/zh-cn/dataitem/dices'

                jsonData = json.dumps([
                    {"dictype": "1", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "2", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "3", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "4", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "5", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "6", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "8", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""}])

                resp = requests.post(link, headers=headers, data=jsonData, verify=False)
                if resp.json().get('data'):
                    # 区域
                    prod_area = []
                    if isinstance(resp.json().get('data').get('1'), list):
                        for item in resp.json().get('data').get('1'):
                            prod_area.append(item.get('Name'))
                        if prod_area:
                            prod_area = str(prod_area).replace("'", '"')
                        else:
                            prod_area = ''
                    else:
                        prod_area = ''

                    # 市场
                    prod_market = []
                    if isinstance(resp.json().get('data').get('2'), list):
                        for item in resp.json().get('data').get('2'):
                            prod_market.append(item.get('Name'))
                        if prod_market:
                            prod_market = str(prod_market).replace("'", '"')
                        else:
                            prod_market = ''
                    else:
                        prod_market = ''

                    # 生产企业
                    prod_factory = []
                    if isinstance(resp.json().get('data').get('3'), list):
                        for item in resp.json().get('data').get('3'):
                            prod_factory.append(item.get('Name'))
                        if prod_factory:
                            prod_factory = str(prod_factory).replace("'", '"')
                        else:
                            prod_factory = ''
                    else:
                        prod_factory = ''

                    # 规格型号
                    prod_specifications = []
                    if isinstance(resp.json().get('data').get('5'), list):
                        for item in resp.json().get('data').get('5'):
                            prod_specifications.append(item.get('Name'))
                        if prod_specifications:
                            prod_specifications = str(prod_specifications).replace("'", '"')
                        else:
                            prod_specifications = ''
                    else:
                        prod_specifications = ''

                    # 用途分类(产品标准)
                    prod_standard = []
                    if isinstance(resp.json().get('data').get('8'), list):
                        for item in resp.json().get('data').get('8'):
                            prod_standard.append(item.get('Name'))
                        if prod_standard:
                            prod_standard = str(prod_standard).replace("'", '"')
                        else:
                            prod_standard = ''
                    else:
                        prod_standard = ''
            except:
                pass

            hashKey = hashlib.md5((str(Type + source_url)).encode("utf8")).hexdigest()  # 数据唯一索引

        elif Type == '国际价格':
            pricetypeid = 34318
            try:
                headers = {
                    'Host': 'prices.sci99.com',
                    'Connection': 'keep-alive',
                    'Content-Length': '1842',
                    'Pragma': 'no-cache',
                    'Cache-Control': 'no-cache',
                    'Accept': '*/*',
                    'Origin': 'https://prices.sci99.com',
                    'X-Requested-With': 'XMLHttpRequest',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                    'Content-Type': 'application/json',
                    'Referer': 'https://prices.sci99.com/cn/product.aspx?ppid=12278&ppname=LDPE&navid=521',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Cookie': 'guid=87ffc1de-3585-c12a-dbed-616c9c4f2d5b; UM_distinctid=17587ca66304c-0f0d4668921dcc-7a1b34-ffc00-17587ca6631b4a; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1606642739,1606676648,1606732555,1607265741; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=llblgsw1t0ktxx5ky1cmycv5; Hm_lvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608090602,1608090609,1608201146,1608265595; STATReferrerIndexId=1; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; href=https%3A%2F%2Fprices.sci99.com%2Fcn%2Fproduct.aspx%3Fppid%3D12278%26ppname%3DLDPE%26navid%3D521; pageViewNum=3; Hm_lpvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608265681',
                }

                link = 'https://prices.sci99.com/api/zh-cn/dataitem/dices'

                jsonData = json.dumps([
                    {"dictype": "1", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "2", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "3", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "4", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "5", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "6", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""},
                    {"dictype": "8", "region": "", "market": "", "factory": "", "model": "", "pname": "",
                     "cycletype": "day", "pricecycle": "", "specialpricetype": "", "ppname": prod_name, "province": "",
                     "pricetypeid": pricetypeid, "ppids": ppid, "navid": navid, "sitetype": 1, "pageno": 1,
                     "pagesize": 300, "purpose": ""}])

                resp = requests.post(link, headers=headers, data=jsonData, verify=False)
                if resp.json().get('data'):
                    # 区域
                    prod_area = []
                    if isinstance(resp.json().get('data').get('1'), list):
                        for item in resp.json().get('data').get('1'):
                            prod_area.append(item.get('Name'))
                        if prod_area:
                            prod_area = str(prod_area).replace("'", '"')
                        else:
                            prod_area = ''
                    else:
                        prod_area = ''

                    # 市场
                    prod_market = []
                    if isinstance(resp.json().get('data').get('2'), list):
                        for item in resp.json().get('data').get('2'):
                            prod_market.append(item.get('Name'))
                        if prod_market:
                            prod_market = str(prod_market).replace("'", '"')
                        else:
                            prod_market = ''
                    else:
                        prod_market = ''

                    # 生产企业
                    prod_factory = []
                    if isinstance(resp.json().get('data').get('3'), list):
                        for item in resp.json().get('data').get('3'):
                            prod_factory.append(item.get('Name'))
                        if prod_factory:
                            prod_factory = str(prod_factory).replace("'", '"')
                        else:
                            prod_factory = ''
                    else:
                        prod_factory = ''

                    # 规格型号
                    prod_specifications = []
                    if isinstance(resp.json().get('data').get('5'), list):
                        for item in resp.json().get('data').get('5'):
                            prod_specifications.append(item.get('Name'))
                        if prod_specifications:
                            prod_specifications = str(prod_specifications).replace("'", '"')
                        else:
                            prod_specifications = ''
                    else:
                        prod_specifications = ''

                    # 用途分类(产品标准)
                    prod_standard = []
                    if isinstance(resp.json().get('data').get('8'), list):
                        for item in resp.json().get('data').get('8'):
                            prod_standard.append(item.get('Name'))
                        if prod_standard:
                            prod_standard = str(prod_standard).replace("'", '"')
                        else:
                            prod_standard = ''
                    else:
                        prod_standard = ''
            except:
                pass

            hashKey = hashlib.md5((str(Type + source_url)).encode("utf8")).hexdigest()  # 数据唯一索引

        else:
            hashKey = None

        insertData = (
            hashKey,
            prod_name,
            Type,
            prod_area if prod_area else '',
            prod_market if prod_market else '',
            prod_factory if prod_factory else '',
            prod_specifications if prod_specifications else '',
            prod_standard if prod_standard else '',
            prod_remark,
            plat_source_id,
            create_time,
            update_time,
            plat_source_url
        )

        updateData = (
            prod_name,
            Type,
            prod_area if prod_area else '',
            prod_market if prod_market else '',
            prod_factory if prod_factory else '',
            prod_specifications if prod_specifications else '',
            prod_standard if prod_standard else '',
            prod_remark,
            plat_source_id,
            update_time,
            plat_source_url,
            hashKey
        )

        if prod_area or prod_market or prod_factory or prod_specifications or prod_standard:
            self.UpdateToMysql(conn, insertSql, insertData, updateSql, updateData)

    """
        获取历史数据
    """

    # 获取历史数据
    def DownloadHistoryData(self, info, history=False):
        HashKey = info.get('hashKey')
        Type = info.get('type')
        DIID = info.get('data').get('DIID')
        DataTypeID = info.get('data').get('DataTypeID')
        PPID = info.get('data').get('ProductID')

        ProductName = quote(info.get('data').get('ProductName').lower())

        Referer = self.detialDownloadUrl.format(DIID, DataTypeID, PPID, ProductName)

        if info.get('data').get('ProductID') in [12547, 12548, 12549, 12550, 12551, 12552, 12553, 12554, 12555, 12710]:
            self.defaultDownloadHeaders.update({
                'Cookie': self.cookie_coll.find_one({'name': 'zc_sj_downloadDetail_second'}).get('cookie'),
                'Referer': Referer
            })
        else:
            self.defaultDownloadHeaders.update({
                'Cookie': self.cookie_coll.find_one({'name': 'zc_sj_downloadDetail'}).get('cookie'),
                'Referer': Referer
            })

        jsonData = {
            "start": "2019/01/01",
            "end": str(time.strftime("%Y-%m-%d", time.localtime(time.time()))).replace('-', '/'),
            "diid": str(DIID),
            "datatypeid": str(DataTypeID),
            "ppid": str(PPID),
            "cycletype": "day",
            "selectconfig": 0
        }

        try:
            resp = requests.post(url=self.defaultDownloadUrl, headers=self.defaultDownloadHeaders, json=jsonData,
                                 timeout=5, verify=False)

            if resp.status_code == 200:
                if resp.json().get('data') and resp.json().get('data').get('List'):
                    items = resp.json().get('data')

                    # 处理数据
                    self.saveDataToFormat(HashKey, Type, Referer, items, history)
                elif resp.json().get('data') and not resp.json().get('data').get('List'):
                    # 代表没有获取到数据
                    # logger.warning('没有获取到数据')
                    self.categoryData_coll.update_one({'hashKey': info['hashKey']}, {'$set': {'status': 400}},
                                                      upsert=True)
                else:
                    # 无返回数据
                    logger.warning('无返回数据')
                    self.categoryData_coll.update_one({'hashKey': info['hashKey']}, {'$set': {'status': 404}},
                                                      upsert=True)
            elif str(resp.status_code).startswith('5'):
                self.categoryData_coll.update_one({'hashKey': info['hashKey']},
                                                  {'$set': {'status': resp.status_code}},
                                                  upsert=True)
                print('网络问题，重试中...')
                return self.DownloadHistoryData(info, history)
            else:
                self.categoryData_coll.update_one({'hashKey': info['hashKey']}, {'$set': {'status': 404}},
                                                  upsert=True)
        except TimeoutError:
            print('网络问题，重试中...')
            return self.DownloadHistoryData(info, history)
        except Exception as error:
            logger.warning(error)
            return

        # 随机休眠
        if not history:
            time.sleep(random.uniform(10, 15))

    # 处理数据
    def saveDataToFormat(self, HashKey, Type, link, info: Dict, history=False):
        PriceType = info.get('DataItem').get('PriceType')
        ProductName = info.get('DataItem').get('ProductName')
        dataList = []

        conn = self.MySql()

        for item in info.get('List'):
            try:
                dataList.append({
                    '日期': item.get('DataDate'),
                    '产品名称': ProductName,
                    '区域': info.get('DataItem').get('Area'),
                    '市场': item.get('MarketSampleName'),
                    '生产企业': item.get('FactorySampleName'),
                    '规格型号': item.get('Model'),
                    '用途': info.get('DataItem').get('Purpose'),
                    '数据类型': PriceType,
                    '最低价': item.get('LDataValue'),
                    '最高价': item.get('HDataValue'),
                    '平均价': item.get('MDataValue'),
                    '涨跌额': item.get('Change') if item.get('Change') else 0,
                    '涨跌率': item.get('ChangeRate') if item.get('Change') else 0,
                    '单位': item.get('Unit'),
                    '价格条件': item.get('PriceCondition'),
                    '备注': item.get('Remark'),
                    'link': link
                })
            except:
                pass

        if dataList:
            if history:
                for data in dataList:
                    self.FormatData(HashKey, conn, Type, link, data)
            else:
                for data in dataList[:8]:
                    self.FormatData(HashKey, conn, Type, link, data)

        # 关闭MySQL连接
        conn.cursor().close()

    # 格式化数据
    def FormatData(self, HashKey, conn, businessType, link, data):
        # 市场价格
        if businessType == '市场价格':
            dt = data.get('日期').replace('/',
                                        '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            dt_type = 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_name = data.get('产品名称')  # 产品名称
            prod_area = data.get('区域')  # 产品区域
            prod_factory = data.get('生产企业')  # 生产企业
            prod_market = data.get('市场')  # 产品市场
            prod_standard = data.get('用途')  # 产品标准(卓创用途分类)
            prod_price_type = data.get('数据类型')  # 价格类型
            prod_specifications = data.get('规格型号')  # 产品规格
            prod_lowest_price = data.get('最低价')  # 最低价
            if prod_lowest_price:
                prod_lowest_price = round(float(prod_lowest_price), 2)
            prod_highest_price = data.get('最高价')  # 最高价
            if prod_highest_price:
                prod_highest_price = round(float(prod_highest_price), 2)
            prod_average_price = data.get('平均价')  # 平均价(报价)
            if prod_average_price:
                prod_average_price = round(float(prod_average_price), 2)
            prod_change_amount = data.get('涨跌额')  # 涨跌额
            if prod_change_amount:
                try:
                    prod_change_amount = round(float(prod_change_amount), 4)
                except:
                    prod_change_amount = 0.00
            prod_change_rate = data.get('涨跌率')  # 涨跌率
            if prod_change_rate:
                try:
                    prod_change_rate = round(float(prod_change_rate), 6)
                except:
                    prod_change_rate = 0.00
            prod_unit = data.get('单位')  # 单位
            price_conditions = data.get('价格条件')  # 价格条件
            lz_rmb_price = ''  # 隆众人民币价格
            prod_remark = data.get('备注')  # 备注
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 2  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = businessType  # 数据来源备注
            plat_source_url = link  # 数据来源网址
            hashKey = hashlib.md5(
                (str(HashKey) + str(businessType) + str(dt) + str(prod_name) + str(prod_specifications)
                 + str(prod_area) + str(prod_factory) + str(prod_market) + str(prod_standard)).encode(
                    "utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO zc_domestic_market_price(hashKey, dt, dt_type, prod_name, prod_area, prod_factory,
            prod_market, prod_standard, prod_price_type, prod_specifications, prod_lowest_price, prod_highest_price, prod_average_price,
            prod_change_amount, prod_change_rate, prod_unit, price_conditions, lz_rmb_price, prod_remark, create_time, update_time,
            plat_source_id, plat_source_remark, plat_source_url)
            VALUES('%s','%s','%d','%s','%s','%s',
            '%s','%s','%s','%s','%f','%f','%f',
            '%f','%f','%s','%s','%s','%s','%s','%s',
            '%d','%s','%s')'''

            insertData = (
                hashKey,
                str(dt),
                int(dt_type),
                str(prod_name) if prod_name else '',
                str(prod_area) if prod_area else '',
                str(prod_factory) if prod_factory else '',
                str(prod_market) if prod_market else '',
                str(prod_standard) if prod_standard else '',
                str(prod_price_type) if prod_price_type else '',
                str(prod_specifications) if prod_specifications else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_highest_price) if prod_highest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                str(prod_unit) if prod_unit else '',
                str(price_conditions) if price_conditions else '',
                str(lz_rmb_price) if lz_rmb_price else '',
                str(prod_remark) if prod_remark else '',
                create_time,
                update_time,
                int(plat_source_id),
                str(plat_source_remark),
                str(plat_source_url)
            )
            # print(insertData)

            updateSql = "update zc_domestic_market_price set dt='%s', dt_type='%d', prod_name='%s', prod_area='%s', prod_factory='%s'," \
                        "prod_market='%s', prod_standard='%s', prod_price_type='%s', prod_specifications='%s', prod_lowest_price='%f', prod_highest_price='%f', prod_average_price='%f', " \
                        "prod_change_amount='%f', prod_change_rate='%f', prod_unit='%s', price_conditions='%s', lz_rmb_price='%s', prod_remark='%s', update_time='%s', " \
                        "plat_source_id='%d', plat_source_remark='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                str(dt),
                int(dt_type),
                str(prod_name) if prod_name else '',
                str(prod_area) if prod_area else '',
                str(prod_factory) if prod_factory else '',
                str(prod_market) if prod_market else '',
                str(prod_standard) if prod_standard else '',
                str(prod_price_type) if prod_price_type else '',
                str(prod_specifications) if prod_specifications else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_highest_price) if prod_highest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                str(prod_unit) if prod_unit else '',
                str(price_conditions) if price_conditions else '',
                str(lz_rmb_price) if lz_rmb_price else '',
                str(prod_remark) if prod_remark else '',
                update_time,
                int(plat_source_id),
                str(plat_source_remark),
                str(plat_source_url),
                hashKey
            )
            # print(updateData)

        # 企业报价
        elif businessType == '企业报价':
            dt = data.get('日期').replace('/',
                                        '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            dt_type = 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_name = data.get('产品名称')  # 产品名称
            prod_area = data.get('区域')  # 产品区域
            prod_market = data.get('市场')  # 产品市场
            prod_factory = data.get('生产企业')  # 生产企业
            prod_sales_company = ''  # 销售公司(默认为空)
            prod_specifications = data.get('规格型号')  # 产品规格
            prod_standard = data.get('用途')  # 产品标准(卓创用途分类)
            price_conditions = data.get('价格条件')  # 价格条件
            prod_lowest_price = data.get('最低价')  # 最低价
            if prod_lowest_price:
                prod_lowest_price = round(float(prod_lowest_price), 2)
            prod_highest_price = data.get('最高价')  # 最高价
            if prod_highest_price:
                prod_highest_price = round(float(prod_highest_price), 2)
            prod_average_price = data.get('平均价')  # 平均价(报价)
            if prod_average_price:
                prod_average_price = round(float(prod_average_price), 2)
            prod_change_amount = data.get('涨跌额')  # 涨跌额
            if prod_change_amount:
                try:
                    prod_change_amount = round(float(prod_change_amount), 4)
                except:
                    prod_change_amount = 0.00
            prod_change_rate = data.get('涨跌率')  # 涨跌率
            if prod_change_rate:
                try:
                    prod_change_rate = round(float(prod_change_rate), 6)
                except:
                    prod_change_rate = 0.00
            prod_unit = data.get('单位')  # 单位
            prod_remark = data.get('备注')  # 备注
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 2  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = businessType  # 数据来源备注
            plat_source_url = link  # 数据来源网址
            hashKey = hashlib.md5(
                (str(HashKey) + str(businessType) + str(dt) + str(prod_name) + str(prod_specifications)
                 + str(prod_area) + str(prod_factory) + str(prod_market) + str(prod_standard)).encode(
                    "utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO zc_factory_produce_price(hashKey, dt, dt_type, prod_name, prod_area, prod_market, prod_factory,
            prod_sales_company, prod_specifications, prod_standard, price_conditions, prod_lowest_price, prod_highest_price,
            prod_average_price, prod_change_amount, prod_change_rate, prod_unit, prod_remark, create_time, update_time,
            plat_source_id, plat_source_remark, plat_source_url)
            VALUES('%s','%s','%d','%s','%s','%s','%s',
            '%s','%s','%s','%s','%f','%f',
            '%f','%f','%f','%s','%s','%s','%s',
            '%d','%s','%s')'''

            insertData = (
                hashKey,
                str(dt),
                int(dt_type),
                str(prod_name) if prod_name else '',
                str(prod_area) if prod_area else '',
                str(prod_market) if prod_market else '',
                str(prod_factory) if prod_factory else '',
                str(prod_sales_company) if prod_sales_company else '',
                str(prod_specifications) if prod_specifications else '',
                str(prod_standard) if prod_standard else '',
                str(price_conditions) if price_conditions else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_highest_price) if prod_highest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                str(prod_unit) if prod_unit else '',
                str(prod_remark) if prod_remark else '',
                create_time,
                update_time,
                int(plat_source_id),
                str(plat_source_remark),
                str(plat_source_url)
            )
            # print(insertData)

            updateSql = "update zc_factory_produce_price set dt='%s', dt_type='%d', prod_name='%s', prod_area='%s', prod_market='%s', prod_factory='%s', " \
                        "prod_sales_company='%s', prod_specifications='%s', prod_standard='%s', price_conditions='%s', prod_lowest_price='%f', prod_highest_price='%f', " \
                        "prod_average_price='%f', prod_change_amount='%f', prod_change_rate='%f', prod_unit='%s', prod_remark='%s', update_time='%s'," \
                        "plat_source_id='%d', plat_source_remark='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                str(dt),
                int(dt_type),
                str(prod_name) if prod_name else '',
                str(prod_area) if prod_area else '',
                str(prod_market) if prod_market else '',
                str(prod_factory) if prod_factory else '',
                str(prod_sales_company) if prod_sales_company else '',
                str(prod_specifications) if prod_specifications else '',
                str(prod_standard) if prod_standard else '',
                str(price_conditions) if price_conditions else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_highest_price) if prod_highest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                str(prod_unit) if prod_unit else '',
                str(prod_remark) if prod_remark else '',
                update_time,
                int(plat_source_id),
                str(plat_source_remark),
                str(plat_source_url),
                hashKey
            )
            # print(updateData)

        # 国际价格
        elif businessType == '国际价格':
            dt = data.get('日期').replace('/',
                                        '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            dt_type = 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_name = data.get('产品名称')  # 产品名称
            prod_area = data.get('区域')  # 产品区域
            prod_factory = data.get('生产企业')  # 生产企业
            prod_market = data.get('市场')  # 产品市场
            prod_standard = data.get('用途')  # 产品标准(卓创用途分类)
            prod_specifications = data.get('规格型号')  # 产品规格
            price_conditions = data.get('价格条件')  # 价格条件
            prod_lowest_price = data.get('最低价')  # 最低价
            if prod_lowest_price:
                prod_lowest_price = round(float(prod_lowest_price), 2)
            prod_highest_price = data.get('最高价')  # 最高价
            if prod_highest_price:
                prod_highest_price = round(float(prod_highest_price), 2)
            prod_average_price = data.get('平均价')  # 平均价(报价)
            if prod_average_price:
                prod_average_price = round(float(prod_average_price), 2)
            prod_change_amount = data.get('涨跌额')  # 涨跌额
            if prod_change_amount:
                try:
                    prod_change_amount = round(float(prod_change_amount), 4)
                except:
                    prod_change_amount = 0.00
            prod_change_rate = data.get('涨跌率')  # 涨跌率
            if prod_change_rate:
                try:
                    prod_change_rate = round(float(prod_change_rate), 6)
                except:
                    prod_change_rate = 0.00
            prod_unit = data.get('单位')  # 单位
            prod_remark = data.get('备注')  # 备注
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 2  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = businessType  # 数据来源备注
            plat_source_url = link  # 数据来源网址
            hashKey = hashlib.md5(
                (str(HashKey) + str(businessType) + str(dt) + str(prod_name) + str(prod_specifications)
                 + str(prod_area) + str(prod_factory) + str(prod_market) + str(prod_standard)).encode(
                    "utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO zc_international_market_price(hashKey, dt, dt_type, prod_name, prod_area, prod_factory,
            prod_market, prod_standard, prod_specifications, price_conditions, prod_lowest_price, prod_highest_price, prod_average_price,
            prod_change_amount, prod_change_rate, prod_unit, prod_remark, create_time,
            update_time, plat_source_id, plat_source_remark, plat_source_url)
            VALUES('%s','%s','%d','%s','%s','%s',
            '%s','%s','%s','%s','%f','%f','%f',
            '%f','%f','%s','%s','%s',
            '%s','%d','%s','%s')'''

            insertData = (
                hashKey,
                str(dt),
                int(dt_type),
                str(prod_name) if prod_name else '',
                str(prod_area) if prod_area else '',
                str(prod_factory) if prod_factory else '',
                str(prod_market) if prod_market else '',
                str(prod_standard) if prod_standard else '',
                str(prod_specifications) if prod_specifications else '',
                str(price_conditions) if price_conditions else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_highest_price) if prod_highest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                str(prod_unit) if prod_unit else '',
                str(prod_remark) if prod_remark else '',
                create_time,
                update_time,
                int(plat_source_id),
                str(plat_source_remark),
                str(plat_source_url)
            )
            # print(insertData)

            updateSql = "update zc_international_market_price set dt='%s', dt_type='%d', prod_name='%s', prod_area='%s', prod_factory='%s', " \
                        "prod_market='%s', prod_standard='%s', prod_specifications='%s', price_conditions='%s', prod_lowest_price='%f', prod_highest_price='%f', prod_average_price='%f', " \
                        "prod_change_amount='%f', prod_change_rate='%f', prod_unit='%s', prod_remark='%s', " \
                        "update_time='%s', plat_source_id='%d', plat_source_remark='%s', plat_source_url='%s'  where hashKey='%s'"

            updateData = (
                str(dt),
                int(dt_type),
                str(prod_name) if prod_name else '',
                str(prod_area) if prod_area else '',
                str(prod_factory) if prod_factory else '',
                str(prod_market) if prod_market else '',
                str(prod_standard) if prod_standard else '',
                str(prod_specifications) if prod_specifications else '',
                str(price_conditions) if price_conditions else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_highest_price) if prod_highest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                str(prod_unit) if prod_unit else '',
                str(prod_remark) if prod_remark else '',
                update_time,
                int(plat_source_id),
                str(plat_source_remark),
                str(plat_source_url),
                hashKey
            )
            # print(updateData)

        else:
            insertData, updateData, insertSql, updateSql = None, None, None, None
            print('businessType 错误')

        # 存储数据
        if insertData and updateData:
            if str(time.strftime("%d", time.localtime(time.time()))) in str(insertData[1][-2:]) or str(
                    time.strftime("%d", time.localtime(time.time()))) in str(updateData[0][-2:]):
                self.categoryData_coll.update_one({'hashKey': HashKey}, {'$set': {'status': 1}}, upsert=True)
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
                logger.warning(updateSql)
                logger.warning(updateData)
                conn.commit()

    # 还原状态
    @staticmethod
    def removeStatus(coll, hashkey):
        for num, info in enumerate(coll.find({'$nor': [{'status': 400}]})):
            print(num)
            coll.update_one({hashkey: info[hashkey]}, {'$unset': {'status': ''}}, upsert=True)

    # 多线程获取数据
    def CommandThread(self, history=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=3)

        # 每周一更新详细类目
        if (pd.to_datetime(str(time.strftime("%Y-%m-%d", time.localtime(time.time())))) - pd.to_datetime('20160103')).days % 7 == 1:
            """
                主类目：68   有数据：68   无数据：0
            """
            self.GetCategory()

            """
                详细分类：3115   有数据：   无数据：
                zc.GetDetailCategory()
            """
            category_list = [i for i in self.category_coll.find({})]
            for info in category_list:
                if Async:
                    out = pool.apply_async(func=self.GetDetailCategory, args=(info,))  # 异步
                else:
                    out = pool.apply(func=self.GetDetailCategory, args=(info,))  # 同步
                thread_list.append(out)

        """
            传入下载日期  xxxx-xx-xx 默认为当天, 有效：2829  不重复: 2604  无权限: 272
        """
        categoryData_list = [i for i in self.categoryData_coll.find({'status': None})]
        for info in categoryData_list:
            if Async:
                out = pool.apply_async(func=self.DownloadHistoryData, args=(info, history,))  # 异步
            else:
                out = pool.apply(func=self.DownloadHistoryData, args=(info, history,))  # 同步
            thread_list.append(out)

        pool.close()
        pool.join()


def zcsjrun():
    zc = ZhuoChuang()

    if str(time.strftime("%H", time.localtime(time.time()))) == '10':
        # 清除标记
        zc.removeStatus(zc.category_coll, 'link')
        zc.removeStatus(zc.categoryData_coll, 'hashKey')

    # 多进程获取数据   params: proxy history
    if (pd.to_datetime(str(time.strftime("%Y-%m-%d", time.localtime(time.time())))) - pd.to_datetime('20160103')).days % 7 not in [6, 7]:
        zc.CommandThread(history=False)

    print('zc 获取历史数据--完成')


if __name__ == '__main__':
    zcsjrun()
