#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import sys
import time

from dingtalkchatbot.chatbot import DingtalkChatbot

sys.path.append("../")
import configparser
import logging
import pprint

import os
from os import path
from fake_useragent import UserAgent

import requests
from pymongo import MongoClient
from multiprocessing.pool import ThreadPool

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/proxies.log'))
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


class HandleProxy:
    def __init__(self):
        # 实例化 Mongo
        proxydb = conf.get("Mongo", "PROXY")
        # proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        proxyclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.API = 'http://webapi.http.zhimacangku.com/getip?num=5&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='

    def GetDisableIps(self):
        disable_ips = self.proxy_coll.find({'$nor': [{'status': 1}]}).sort('_id', -1)
        self.ThreadTestIp(disable_ips)

    def ThreadTestIp(self, ip_list, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=8)

        for ip_info in ip_list:
            if Async:
                out = pool.apply_async(func=self.Checker, args=(ip_info,))  # 异步
            else:
                out = pool.apply(func=self.Checker, args=(ip_info,))  # 同步
            thread_list.append(out)

        pool.close()
        pool.join()

    def Checker(self, ip_info):
        pro = ip_info.get('pro')
        try:
            # 请求地址
            targetUrl = "https://plas.chem99.com/"

            # 代理服务器
            proxyHost = pro.split(':')[0]
            proxyPort = pro.split(':')[1]

            proxyMeta = "http://%(host)s:%(port)s" % {

                "host": proxyHost,
                "port": proxyPort,
            }

            proxies = {
                "http": proxyMeta,
                "https": proxyMeta
            }

            header = {
                'authority': 'plas.chem99.com',
                'method': 'GET',
                'path': '/',
                'scheme': 'https',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': UserAgent().random
            }

            resp = requests.get(targetUrl, headers=header, proxies=proxies, timeout=5)
            if resp.status_code == 200:
                self.proxy_coll.update_one({'pro': pro}, {'$set': {
                    'status': 1,
                    'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                }}, upsert=True)
            else:
                self.proxy_coll.update_one({'pro': pro}, {'$set': {
                    'status': resp.status_code,
                    'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                }}, upsert=True)
        except TimeoutError:
            self.proxy_coll.update_one({'pro': pro}, {'$set': {
                'status': 404,
                'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
            }}, upsert=True)
        except:
            self.proxy_coll.update_one({'pro': pro}, {'$set': {
                'status': resp.status_code,
                'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
            }}, upsert=True)


if __name__ == '__main__':
    hp = HandleProxy()
    hp.GetDisableIps()