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
import pandas as pd

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
        self.API = 'http://webapi.http.zhimacangku.com/getip?num=10&type=1&pro=0&city=0&yys=0&port=1&time=2&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='

    def InsertProxy(self, times):
        for t in range(times):
            try:
                resp = requests.get(self.API)
                resp.encoding = 'utf-8'

                for pro in resp.text.strip().split('\n'):
                    try:
                        if ('秒' or '添加' or '余额') not in pro:
                            self.proxy_coll.update_one({'pro': pro}, {'$set': {
                                'pro': pro.replace('\n', '').replace('\t', '').replace('\r', ''),
                                'create_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                            }}, upsert=True)
                        elif '余额' in pro:
                            self.SendMeaasge('代理账户余额不足，请充值！！！')
                            return
                    except Exception as error:
                        logger.warning(error)
            except Exception as error:
                logger.warning(error)
            time.sleep(5)
            break

        # 检测新插入的代理
        create_ips = self.proxy_coll.find({'status': None})
        self.ThreadTestIp(create_ips)

    def SelectCount(self):
        # old_ips = self.proxy_coll.find({'status': 1})
        # self.ThreadTestIp(old_ips)

        new_count = self.proxy_coll.find({'status': 1}).count()

        if 0 <= int(new_count) <= 10:
            # 每周末不使用代理
            if (pd.to_datetime(str(time.strftime("%Y-%m-%d", time.localtime(time.time())))) - pd.to_datetime('20160103')).days % 7 not in [6, 7]:
                # 每天10：00-19：00  少于40个可用则补充
                if 10 <= int(time.strftime("%H", time.localtime(time.time()))) <= 19:
                    # 代理消耗完报警,检查代理供应商账户余额
                    if int(new_count) == 0:
                        pass
                        # self.SendMeaasge('服务器代理消耗完毕， 请及时补充！！！')

                    # times = int((30 - int(new_count)) / 10) + 1
                    times = 1
                    self.InsertProxy(times)
        else:
            pass

    def SendMeaasge(self, msg, retry=2):
        webhook = 'https://oapi.dingtalk.com/robot/send?' \
                  'access_token=ae5ee6aad7142340e40194f90b0bfcfed510e568ccfb781e942e18deb7195ea2'

        time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        try:
            message = DingtalkChatbot(webhook)
            message.send_markdown(
                title="SpiderStatus",
                text="Date: {time}\n\n"
                     "------------------------------------\n\n\n"
                     "  {msg} \n\n\n".format(
                    time=time_now,
                    msg=msg
                )
            )
        except Exception as e:
            logging.warning(e)
            if retry > 0:
                return self.SendMeaasge(retry - 1)
            else:
                logging.warning('The number of retries has been exhaustes !')
                pass

    def ThreadTestIp(self, ip_list, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=3)

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
        except Exception as error:
            print(error)


if __name__ == '__main__':
    hp = HandleProxy()
    # hp.SelectCount()
    hp.InsertProxy(1)