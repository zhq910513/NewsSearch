#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

import sys
import threading

sys.path.append("../")
import configparser
import hashlib
import logging
import os
import pprint
import random
import time
from os import path
from urllib.parse import quote

import requests
import pandas as pd
from fake_useragent import UserAgent
from pymongo import MongoClient
import pymysql
from pymysql.err import IntegrityError

from Cookies.GetCookie import Cookie

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/zhuochuang_zs.log'))
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

        self.category_coll = client[datadb]['zc_zs_category']

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

        # 请求头信息
        self.userAgent = UserAgent().random
        self.categoryUrl = 'https://index.sci99.com/api/nav/zh-cn/2?_={}'
        self.categoryHeaders = {
            'authority': 'index.sci99.com',
            'method': 'GET',
            'scheme': 'https',
            'accept': 'text/plain, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'content-type': 'application/x-www-form-urlencoded',
            'pragma': 'no-cache',
            'referer': 'https://index.sci99.com/channel/product/path2/%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99%E4%BB%B7%E6%A0%BC%E6%8C%87%E6%95%B0/3.html',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': self.userAgent
        }
        self.categoryDataUrl = 'https://index.sci99.com/api/zh-cn/dataitem/datavalue'
        self.categoryDataHeaders = {
            'authority': 'index.sci99.com',
            'method': 'POST',
            'path': '/api/zh-cn/dataitem/datavalue',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'content-length': '98',
            'content-type': 'application/json',
            'origin': 'https://index.sci99.com',
            'pragma': 'no-cache',
            'referer': 'https://index.sci99.com/channel/product/hy/%E5%A1%91%E6%96%99/3.html',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }

        # 实例化 Mysql
        host = conf.get("Mysql", "HOST")
        mysqldb = 'plastic_price'
        user = conf.get("Mysql", "USER")
        pwd = conf.get("Mysql", "PASSWORD")
        self.conn = pymysql.Connect(
            host=host,
            port=3306,
            user=user,
            password=pwd,
            db=mysqldb,
            charset='utf8mb4',
            autocommit=True
        )
        self.cursor = self.conn.cursor()

    def GetProxy(self):
        try:
            if self.pros:
                usePro = self.pros.pop()
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

    """
        获取主类目
    """

    # 获取类目
    def GetCategory(self, proxy=False):
        currentTime = int(round(time.time() * 1000))
        link = self.categoryUrl.format(currentTime)
        self.categoryHeaders.update({'path': '/api/nav/zh-cn/2?_={}'.format(currentTime)})
        try:
            self.categoryHeaders.update({
                'Cookie': self.cookie_coll.find_one({'name': 'zc_zs_category'}).get('cookie')
            })
            if proxy:
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(link, headers=self.categoryHeaders, proxies=pro, timeout=5, verify=False)
                else:
                    resp = requests.get(link, headers=self.categoryHeaders, timeout=5, verify=False)
            else:
                resp = requests.get(link, headers=self.categoryHeaders, timeout=5, verify=False)
            if resp.status_code == 200:
                if resp.json() and isinstance(resp.json(), list):
                    for item in resp.json()[98:120]:
                        try:
                            self.category_coll.update_one({'Name': item['Name']}, {'$set': item}, upsert=True)
                        except Exception as error:
                            logger.warning(error)
            else:
                logger.warning(resp.status_code)
        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetCategory(proxy)
        except TimeoutError:
            logger.warning(link)
        except Exception as error:
            logger.warning(error)
            return None

    """
        获取历史数据
    """

    # 获取类目下的数据 默认一年
    def DownloadCategoryData(self, proxy=False, history=False):
        for info in self.category_coll.find({'status': None}).batch_size(3):
            # 主类目
            if info.get('Hy'):
                Referer = 'https://index.sci99.com/channel/product/hy/{}/3.html'.format(quote(info.get('Hy')))
                Hy = info.get('Hy')
                Level = 0
                Path1 = info.get('Name')
                Path2 = ''
                Path3 = ''
                Path4 = ''
            # 一级类目
            elif not info.get('Hy') and info.get('ID')[-1] not in ['2', '3', '4', '5', '6', '7', '8', '9', '10'] and \
                    info.get('PID')[-1] not in ['3', '4', '5', '6', '7', '8', '9', '10']:
                Referer = 'https://index.sci99.com/channel/product/path2/{}/1.html'.format(quote(info.get('Name')))
                Hy = '塑料'
                Level = 1
                Path1 = info.get('PID')
                Path2 = info.get('Name')
                Path3 = ''
                Path4 = ''
            # 二级类目
            elif not info.get('Hy') and info.get('ID')[-1] in ['2', '3', '4', '5', '6', '7', '8', '9', '10']:
                Referer = 'https://index.sci99.com/channel/product/path3/%E8%81%9A%E7%83%AF%E7%83%83%E4%BB%B7%E6%A0%BC%E6%8C%87%E6%95%B0/1.html'
                Hy = '塑料'
                Level = 2
                Path1 = '塑料价格指数'
                Path2 = info.get('PID')
                Path3 = info.get('Name')
                Path4 = ''
            # 三级类目
            elif not info.get('Hy') and info.get('PID')[-1] in ['3', '4', '5', '6', '7', '8', '9', '10']:
                ParentInfo = self.category_coll.find_one({'ID': info.get('PID')})
                Referer = 'https://index.sci99.com/channel/product/path4/{}/1.html'.format(quote(info.get('Name')))
                Hy = '塑料'
                Level = 3
                Path1 = '塑料价格指数'
                Path2 = ParentInfo.get('PID')
                Path3 = ParentInfo.get('Name')
                Path4 = info.get('Name')
            else:
                Hy = None
                Referer = None
                Level = None
                Path1 = None
                Path2 = None
                Path3 = None
                Path4 = None

            self.categoryDataHeaders.update({
                'Cookie': self.cookie_coll.find_one({'name': 'zc_zs_downloadDetail'}).get('cookie'),
                'referer': Referer
            })

            jsonData = {
                'hy': Hy,
                'level': Level,
                'path1': Path1,
                'path2': Path2,
                'path3': Path3,
                'path4': Path4,
                'type': "3"
            }

            try:
                if proxy:
                    pro = self.GetProxy()
                    if pro:
                        resp = requests.post(url=self.categoryDataUrl, headers=self.categoryDataHeaders, proxies=pro,
                                             json=jsonData, timeout=5, verify=False)
                    else:
                        resp = requests.post(url=self.categoryDataUrl, headers=self.categoryDataHeaders, json=jsonData,
                                             timeout=5, verify=False)
                else:
                    resp = requests.post(url=self.categoryDataUrl, headers=self.categoryDataHeaders, json=jsonData,
                                         timeout=5, verify=False)
                if resp.status_code == 200:
                    if resp.json().get('List') and isinstance(resp.json().get('List'), list):
                        if history:
                            for item in resp.json().get('List'):
                                self.FormatData(Referer, item)
                        else:
                            for item in resp.json().get('List')[:3]:
                                self.FormatData(Referer, item)

                        # 标记已抓取数据
                        self.category_coll.update_one({'Name': info.get('Name')}, {'$set': {'status': 1}}, upsert=True)
                    else:
                        logger.warning('没有数据， 请更换cookie')
                else:
                    pp.pprint(resp.text)
            except requests.exceptions.ConnectionError:
                threading.Thread(target=self.DisProxy, args=(pro,)).start()
                print('网络问题，重试中...')
                return self.DownloadCategoryData(proxy)
            except TimeoutError:
                logger.warning(Referer)
            except Exception as error:
                logger.warning(error)

            # 随机休眠
            time.sleep(random.uniform(5, 10))

            # break

        print('zc_zs 获取历史数据--完成')

        # 关闭MySQL
        self.cursor.close()

        # 清除已读数据标记
        self.removeStatus(self.category_coll, 'Name')

    # 格式化数据
    def FormatData(self, link, data):
        try:
            dt = data.get('DataDate').replace('/',
                                              '')  # 数据日期(每日报价格式人如20201209，周均价使用区间如20201105-20201112，月均价格式为202011，年均价格式为2020)
            index_name = data.get('DataName')  # 指数名称
            index_value = data.get('MDataValue')  # 指数值
            index_change_amount = data.get('Change')  # 涨跌值
            index_change_rate = data.get('ChangeRate')  # 涨跌幅
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 2  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_remark = '卓创指数'  # 数据来源备注
            plat_source_url = link  # 数据来源网址
            hashKey = hashlib.md5(str(index_name + dt).encode("utf8")).hexdigest()  # 数据唯一索引

            insertSql = '''INSERT INTO zc_plastic_price_index(hashKey, dt, index_name, index_value, index_change_amount,
            index_change_rate, create_time, update_time, plat_source_id, plat_source_remark, plat_source_url)
            VALUES('%s','%s','%s','%f','%f','%f','%s','%s','%d','%s','%s')'''

            insertData = (
                hashKey,
                dt if dt else '',
                index_name if index_name else '',
                index_value if index_value else 0.00,
                index_change_amount if index_change_amount else 0.00,
                index_change_rate if index_change_rate else 0.00,
                create_time,
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url
            )
            # print(insertData)

            updateSql = "update zc_plastic_price_index set dt='%s', index_name='%s', index_value='%f', index_change_amount='%f', " \
                        "index_change_rate='%f', update_time='%s', plat_source_id='%d', plat_source_remark='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                dt if dt else '',
                index_name if index_name else '',
                index_value if index_value else 0.00,
                index_change_amount if index_change_amount else 0.00,
                index_change_rate if index_change_rate else 0.00,
                update_time,
                plat_source_id,
                plat_source_remark,
                plat_source_url,
                hashKey
            )
            # print(updateData)

            self.UpdateToMysql(insertSql, insertData, updateSql, updateData)
        except Exception as error:
            logger.warning(error)

    # 更新数据到MySQL
    def UpdateToMysql(self, insertSql, insertData, updateSql, updateData):
        try:
            self.cursor.execute(insertSql % insertData)
            print(insertData)
        except IntegrityError:
            try:
                self.cursor.execute(updateSql % updateData)
                print(updateData)
            except Exception as error:
                logger.warning(error)
        finally:
            self.conn.commit()

    # 还原状态
    @staticmethod
    def removeStatus(coll, key):
        for info in coll.find({'$nor': [{'status': 404}, {'status': 400}]}):
            coll.update_one({key: info[key]}, {'$unset': {'status': ''}}, upsert=True)


def zczsrun():
    Cookie().ChoicePlatform()

    start_time = time.time()
    zc = ZhuoChuang()

    # 主类目：22   有数据：22   无数据：0    每周天更新
    if (pd.to_datetime(str(time.strftime("%Y-%m-%d", time.localtime(time.time())))) - pd.to_datetime('20160103')).days % 7 == 0:
        zc.GetCategory(proxy=False)

    # 详细分类：22   有数据：22   无数据：0
    zc.DownloadCategoryData(proxy=True, history=False)
    end_time = time.time()
    logger.warning(end_time - start_time)


if __name__ == '__main__':
    zczsrun()
