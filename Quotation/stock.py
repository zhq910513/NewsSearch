#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import copy
import random
import sys

from pymongo import MongoClient

sys.path.append("../")
import configparser
import requests
import time
import logging
import os
from os import path
import json
import pprint

import pymysql
import hashlib
from fake_useragent import UserAgent

pp = pprint.PrettyPrinter(indent=4)
requests.packages.urllib3.disable_warnings()

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))
logPath = os.path.abspath(os.path.join(dh + r'/Logs/Stock.log'))

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


class GetStockData:
    def __init__(self):
        # 实例化 Mongo
        proxydb = conf.get("Mongo", "PROXY")
        # proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        proxyclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = copy.deepcopy(self.proxy_coll.find({'status': 1}))

        # 实例化 Mysql
        host = conf.get("Mysql", "HOST")
        mysqldb = conf.get("Mysql", "MYSQLDB")
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
        if not self.pros:
            self.pros = self.proxy_coll.find({'status': 1})
        try:
            usePro = random.choice(list(self.pros))
            if usePro.get('pro'):
                return {
                    'http': 'http://{}'.format(usePro.get('pro')),
                    'https': 'http://{}'.format(usePro.get('pro')),
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

    # NYMEX原油行情
    def GetClooyQuotation(self, proxy=False):
        try:
            headers = {
                'Referer': 'http://quote.eastmoney.com/globalfuture/B00Y.html',
                'User-Agent': UserAgent().random
            }

            timeStamp = int(time.time() * 1000)

            if proxy:
                pro = self.GetProxy()
                if pro:
                    response = requests.get(
                        url='http://futsse.eastmoney.com/static/102_CL00Y_qt?callbackName=aa&cb=aa&_={}'.format(
                            timeStamp), headers=headers, proxies=pro, verify=False)
                else:
                    response = requests.get(
                        url='http://futsse.eastmoney.com/static/102_CL00Y_qt?callbackName=aa&cb=aa&_={}'.format(
                            timeStamp), headers=headers, verify=False)
            else:
                response = requests.get(
                    url='http://futsse.eastmoney.com/static/102_CL00Y_qt?callbackName=aa&cb=aa&_={}'.format(timeStamp),
                    headers=headers, verify=False)
            # print(response.text)

            # 得到response返回信息
            infoJson = json.loads(response.text.split('(')[1].split(')')[0])
            data = {
                'hashKey': 'booy' + str(timeStamp),
                # 'utime': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-334))),
                'utime': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(infoJson['qt'].get('utime')))),
                'futures_jyzt': infoJson['qt'].get('jyzt'),
                'futures_spsj': infoJson['qt'].get('spsj'),
                '代码': infoJson['qt'].get('dm'),
                'Name': infoJson['qt'].get('name'),
                '最新': round(infoJson['qt'].get('p'), 2),
                '昨收': round(infoJson['qt'].get('zjsj'), 2),
                '总量': round(infoJson['qt'].get('vol') / 10000, 2),
                '持仓': round(infoJson['qt'].get('ccl') / 10000, 2),
                '涨幅': round(infoJson['qt'].get('zdf'), 2),
                '涨跌额': round(infoJson['qt'].get('zde'), 2),
                '最高': round(infoJson['qt'].get('h'), 2),
                '最低': round(infoJson['qt'].get('l'), 2),
                '外盘': round(infoJson['qt'].get('wp'), 2),
                '内盘': round(infoJson['qt'].get('np'), 2),
                '前结': round(infoJson['qt'].get('qrspj'), 2),
                '日增': round(infoJson['qt'].get('rz'), 2)
            }
            return self.FormatData(data)
        except Exception as error:
            logger.warning(error)
            pass

    # 布伦特原油行情
    def GetBooyQuotation(self, proxy=False):
        try:
            headers = {
                'Referer': 'http://quote.eastmoney.com/globalfuture/B00Y.html',
                'User-Agent': UserAgent().random
            }

            timeStamp = int(time.time() * 1000)

            if proxy:
                pro = self.GetProxy()
                if pro:
                    response = requests.get(
                        url='http://futsse.eastmoney.com/static/112_B00Y_qt?callbackName=aa&cb=aa&_={}'.format(
                            timeStamp), headers=headers, proxies=pro, verify=False)
                else:
                    response = requests.get(
                        url='http://futsse.eastmoney.com/static/112_B00Y_qt?callbackName=aa&cb=aa&_={}'.format(
                            timeStamp), headers=headers, verify=False)
            else:
                response = requests.get(
                    url='http://futsse.eastmoney.com/static/112_B00Y_qt?callbackName=aa&cb=aa&_={}'.format(timeStamp),
                    headers=headers, verify=False)
            # print(response.text)

            # 得到response返回信息
            infoJson = json.loads(response.text.split('(')[1].split(')')[0])
            data = {
                'hashKey': 'booy' + str(timeStamp),
                # 'utime': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-334))),
                'utime': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(infoJson['qt'].get('utime')))),
                'futures_jyzt': infoJson['qt'].get('jyzt'),
                'futures_spsj': infoJson['qt'].get('spsj'),
                '代码': infoJson['qt'].get('dm'),
                'Name': infoJson['qt'].get('name'),
                '最新': round(infoJson['qt'].get('p'), 2),
                '昨收': round(infoJson['qt'].get('zjsj'), 2),
                '总量': round(infoJson['qt'].get('vol') / 10000, 2),
                '持仓': round(infoJson['qt'].get('ccl') / 10000, 2),
                '涨幅': round(infoJson['qt'].get('zdf'), 2),
                '涨跌额': round(infoJson['qt'].get('zde'), 2),
                '最高': round(infoJson['qt'].get('h'), 2),
                '最低': round(infoJson['qt'].get('l'), 2),
                '外盘': round(infoJson['qt'].get('wp'), 2),
                '内盘': round(infoJson['qt'].get('np'), 2),
                '前结': round(infoJson['qt'].get('qrspj'), 2),
                '日增': round(infoJson['qt'].get('rz'), 2)
            }
            return self.FormatData(data)
        except Exception as error:
            logger.warning(error)
            pass

    # 格式化数据
    def FormatData(self, data):
        hashKey = hashlib.md5(str(data.get('hashKey')).encode("utf8")).hexdigest()  # 数据唯一索引
        futures_code = data.get('代码')  # 期货代码
        futures_name = data.get('Name')  # 期货名称
        dt_datetime = data.get('utime')  # 时间
        futures_jyzt = data.get('futures_jyzt')  # 期货交易状态
        futures_spsj = data.get('futures_spsj')  # 期货交易状态
        current = data.get('最新')  # 最新
        pre_close = data.get('昨收')  # 昨收
        volume = data.get('总量')
        position = data.get('持仓')
        change_pct = data.get('涨幅')
        change_amout = data.get('涨跌额')
        high = data.get('最高')
        low = data.get('最低')
        sell = data.get('外盘')
        buy = data.get('内盘')
        day_delta = data.get('前结')
        pre_sett_price = data.get('日增')
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        insert_sql = '''INSERT INTO securities_price_info(hashKey, futures_code, futures_name, dt_datetime, current, pre_close, volume, position, change_pct, change_amout, high, low, sell, buy, day_delta, pre_sett_price, create_time, update_time, futures_jyzt, futures_spsj)
        VALUES('%s','%s','%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%s','%s', '%d', '%d')'''

        sqlData = (
            hashKey,
            futures_code,
            futures_name,
            dt_datetime,
            current if current else 0.00,
            pre_close if pre_close else 0.00,
            volume if volume else 0.00,
            position if position else 0.00,
            change_pct if change_pct else 0.00,
            change_amout if change_amout else 0.00,
            high if high else 0.00,
            low if low else 0.00,
            sell if sell else 0.00,
            buy if buy else 0.00,
            day_delta if day_delta else 0.00,
            pre_sett_price if pre_sett_price else 0.00,
            create_time,
            update_time,
            futures_jyzt,
            futures_spsj
        )
        print(insert_sql)
        print(sqlData)

        self.UpdateToMysql(insert_sql, sqlData)

    # 更新数据到MySQL
    def UpdateToMysql(self, sql, data):
        try:
            self.cursor.execute(sql % data)
        except Exception as error:
            if 'Duplicate entry' in str(error):
                pass
            else:
                logger.warning(error)
        finally:
            self.conn.commit()

    # 执行程序
    def run(self):
        # NYMEX原油行情
        self.GetClooyQuotation(False)

        # 布伦特原油行情
        self.GetBooyQuotation(False)


if __name__ == '__main__':
    stc = GetStockData()
    stc.run()
