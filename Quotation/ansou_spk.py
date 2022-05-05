#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import sys
import threading
from multiprocessing.pool import ThreadPool

import pymysql

sys.path.append("../")
import configparser
import hashlib
import logging
import os
import pprint
import time
import re
from os import path
import json

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/ansou_spk.log'))
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


class AnsouSPK:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")
        proxydb = conf.get("Mongo", "PROXY")

        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))

        cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        # proxyclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]

        self.factory_coll = client[datadb]['ansou_factory']
        self.wuxing_coll = client[datadb]['ansou_wuxing']
        self.supplier_coll = client[datadb]['ansou_supplier']
        self.supplier_detail_coll = client[datadb]['ansou_supplier_detail']

        self.factory_url = 'https://www.antsoo.com/plastic/producer?page={}'
        self.factory_headers = {
            'authority': 'www.antsoo.com',
            'method': 'GET',
            'path': '/plastic/producer?page=2',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'referer': 'https://www.antsoo.com/plastic/producer?page=2',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.wuxing_api = 'https://www.antsoo.com/api/materials/materialsPropDetail'
        self.wuxing_headers = {
            'Host': 'www.antsoo.com',
            'Connection': 'keep-alive',
            'Content-Length': '40',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://www.antsoo.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MTYzNDgzMTQsInVzZXJJZCI6IkVFQUZGQiJ9.oqb6TbbUS57w1NSRP3PmyICnMnHBk9JEtH_hQkAXy7M',
            'Content-Type': 'application/json;charset=UTF-8',
            'Referer': 'https://www.antsoo.com/physical/detail?id=124145&from=producer',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cookie': 'AGL_USER_ID=1baa6b41-49f0-4e4a-9dfc-e385a4c45dab; Hm_lvt_81c6de53553d9a1245777581c2a0bf7c=1616228034; acw_tc=0e1d289516163482670734180e95499e6aeac443c4f93a4cb18205d7e8; antUserInfo=%7B%22id%22%3A115226%2C%22username%22%3A%2219925137464%22%2C%22password%22%3A%22420ae8a5bedd99b9ae025a6e8cebf077%22%2C%22name%22%3Anull%2C%22userLogo%22%3A%22https%3A%2F%2F139oss.oss-cn-shanghai.aliyuncs.com%2Fapp%2Fdefault%2FuserNewImage.png%22%2C%22attendtionLines%22%3Anull%2C%22userType%22%3A1%2C%22companyCode%22%3Anull%2C%22opeIndustry%22%3Anull%2C%22companyName%22%3Anull%2C%22companyWebsite%22%3Anull%2C%22status%22%3A%221%22%2C%22websitStatus%22%3Anull%2C%22accreditStatus%22%3Anull%2C%22qq%22%3Anull%2C%22card%22%3Anull%2C%22province%22%3Anull%2C%22city%22%3Anull%2C%22area%22%3Anull%2C%22address%22%3Anull%2C%22email%22%3Anull%2C%22position%22%3Anull%2C%22isRelevance%22%3Anull%2C%22isCheck%22%3Anull%2C%22createDate%22%3A1616312410000%2C%22lastUpdateDate%22%3A1616312410000%2C%22enterpriseId%22%3Anull%2C%22source%22%3A%228%22%2C%22businessMode%22%3Anull%2C%22score%22%3A0%2C%22isPublicTel%22%3A1%2C%22ticket%22%3Anull%2C%22attentionLabels%22%3Anull%2C%22attentionMaterial%22%3Anull%2C%22isTakeMemberCard%22%3A1%2C%22lockedMemberCount%22%3A0%2C%22lookEnterpriseCount%22%3A0%2C%22lookTopicsCount%22%3A0%2C%22lookExponentCount%22%3A0%2C%22isShowClue%22%3A0%2C%22globalRoaming%22%3A%2286%22%2C%22newUserEffectForce%22%3Anull%2C%22newUserEffectForceRank%22%3A%220.0%25%22%7D; antToken=%22eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MTYzNDgzMTQsInVzZXJJZCI6IkVFQUZGQiJ9.oqb6TbbUS57w1NSRP3PmyICnMnHBk9JEtH_hQkAXy7M%22; workStatus=1; Hm_lpvt_81c6de53553d9a1245777581c2a0bf7c=1616348331'
        }

        self.supplier_api = 'https://www.antsoo.com/api/producer/supplierList'
        self.supplier_headers = {
            'authority': 'www.antsoo.com',
            'method': 'POST',
            'path': '/api/producer/supplierList',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'content-length': '107',
            'content-type': 'application/json;charset=UTF-8',
            'cookie': 'AGL_USER_ID=1baa6b41-49f0-4e4a-9dfc-e385a4c45dab; acw_tc=3b258e9c16163769287715991e59ff96228fd3c1256e082cd5b679cefb; Hm_lvt_81c6de53553d9a1245777581c2a0bf7c=1616228034; antUserInfo=%7B%22id%22%3A115226%2C%22username%22%3A%2219925137464%22%2C%22password%22%3A%22420ae8a5bedd99b9ae025a6e8cebf077%22%2C%22name%22%3Anull%2C%22userLogo%22%3A%22https%3A%2F%2F139oss.oss-cn-shanghai.aliyuncs.com%2Fapp%2Fdefault%2FuserNewImage.png%22%2C%22attendtionLines%22%3Anull%2C%22userType%22%3A1%2C%22companyCode%22%3Anull%2C%22opeIndustry%22%3Anull%2C%22companyName%22%3Anull%2C%22companyWebsite%22%3Anull%2C%22status%22%3A%221%22%2C%22websitStatus%22%3Anull%2C%22accreditStatus%22%3Anull%2C%22qq%22%3Anull%2C%22card%22%3Anull%2C%22province%22%3Anull%2C%22city%22%3Anull%2C%22area%22%3Anull%2C%22address%22%3Anull%2C%22email%22%3Anull%2C%22position%22%3Anull%2C%22isRelevance%22%3Anull%2C%22isCheck%22%3Anull%2C%22createDate%22%3A1616312410000%2C%22lastUpdateDate%22%3A1616312410000%2C%22enterpriseId%22%3Anull%2C%22source%22%3A%228%22%2C%22businessMode%22%3Anull%2C%22score%22%3A0%2C%22isPublicTel%22%3A1%2C%22ticket%22%3Anull%2C%22attentionLabels%22%3Anull%2C%22attentionMaterial%22%3Anull%2C%22isTakeMemberCard%22%3A1%2C%22lockedMemberCount%22%3A0%2C%22lookEnterpriseCount%22%3A0%2C%22lookTopicsCount%22%3A0%2C%22lookExponentCount%22%3A0%2C%22isShowClue%22%3A0%2C%22globalRoaming%22%3A%2286%22%2C%22newUserEffectForce%22%3Anull%2C%22newUserEffectForceRank%22%3A%220.0%25%22%7D; antToken=%22eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MTYzNzcwMTMsInVzZXJJZCI6IkVFQUZGQiJ9.15TSmO-Hz9Lko5jplMP9bwb1YfM6ZlbH9C9VExD3Lq4%22; workStatus=1; Hm_lpvt_81c6de53553d9a1245777581c2a0bf7c=1616377198',
            'origin': 'https://www.antsoo.com',
            'pragma': 'no-cache',
            'referer': 'https://www.antsoo.com/plastic/producer/producersupplier?name=4188&id=124145&category=2476&exponentId=16&producer=2621&model=12774',
            'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MTYzNzcwMTMsInVzZXJJZCI6IkVFQUZGQiJ9.15TSmO-Hz9Lko5jplMP9bwb1YfM6ZlbH9C9VExD3Lq4',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        self.downloadPdfHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': '139oss.oss-cn-shanghai.aliyuncs.com',
            'Pragma': 'no-cache',
            'Referer': 'https://www.antsoo.com/physical/detailPDF?name=PC%2F%E5%8F%B0%E5%8C%96%E5%87%BA%E5%85%89%2FIR2200&pdfUrl=https%3A%2F%2F139oss.oss-cn-shanghai.aliyuncs.com%2Findustry%2Fir2200_98e5aa55-eb28-44b4-a158-2f7f1e09e178.pdf',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

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

    # 获取所有厂家数据
    def GetChangjiaMessages(self, pageNum=1, proxy=False):
        print('第 {} 页'.format(pageNum))
        try:
            link = self.factory_url.format(pageNum)
            print(link)
            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url=link, headers=self.factory_headers, proxies=pro, timeout=5, verify=False)
                else:
                    resp = requests.post(url=link, headers=self.factory_headers, timeout=5, verify=False)
            else:
                resp = requests.post(url=link, headers=self.factory_headers, timeout=5, verify=False)

            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data, maxpage = self.ParseMessages(resp.text)
                if data and isinstance(data, list):
                    for msg in data:
                        msg['factory_link'] = msg.get('factory_link') + '&page={}'
                        msg.update({
                            'factory_hashkey': hashlib.md5(msg['factory_link'].encode("utf8")).hexdigest()
                        })
                        self.factory_coll.update_one({'factory_link': msg['factory_link']}, {'$set': msg}, upsert=True)

                if pageNum <= maxpage:
                    return self.GetChangjiaMessages(pageNum + 1, proxy)
                else:
                    print('已经最大页')
            else:
                print(resp.status_code)

        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetChangjiaMessages(pageNum, proxy)
        except TimeoutError:
            logger.warning('第 {} 页 TimeoutError'.format(pageNum))
        except Exception as error:
            logger.warning(error)
            return

    # 解析最新数据或者历史数据
    @staticmethod
    def ParseMessages(Html):
        soup = BeautifulSoup(Html, 'lxml')
        dataList = []
        try:
            if soup.find('table'):
                titles = [th.get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip() for th in
                          soup.find('table').find_all('tr')[0].find_all('th')]

                for tr in soup.find('table').find_all('tr')[1:]:
                    data = {}
                    try:
                        if len(titles) == len(tr.find_all('td')):
                            for n in range(len(titles)):
                                key = titles[n]
                                if key == '操作':
                                    key = 'factory_link'
                                try:
                                    if tr.find_all('td')[n].find('img'):
                                        value = tr.find_all('td')[n].find('img').get('src')
                                    else:
                                        value = tr.find_all('td')[n].get_text().replace('\n', '').replace('\t',
                                                                                                          '').replace(
                                            '\r', '').strip()
                                        if value == '查看详情':
                                            value = tr.find_all('td')[n].find('a').get('href')
                                            if not str(value).startswith('https://www.antsoo.com'):
                                                value = 'https://www.antsoo.com' + value
                                except:
                                    value = None

                                if key == '厂家信息':
                                    try:
                                        data.update({
                                            '厂家名称': tr.find_all('td')[n].find('h2').get_text(),
                                            '国别地区': tr.find_all('td')[n].find('p').get_text()
                                        })
                                    except:
                                        pass
                                else:
                                    data.update({
                                        key: value
                                    })
                        if data:
                            dataList.append(data)
                    except Exception as error:
                        logger.warning(tr)
                        logger.warning(error)

            else:
                logger.warning('没有厂家信息表')
        except Exception as error:
            logger.warning(error)

        # 最大页
        try:
            maxPage = re.findall('共(\d+)条，(\d+)页', Html, re.S)
            if maxPage:
                maxPage = int(maxPage[0][1])
            else:
                maxPage = 0
        except:
            maxPage = 0

        return dataList, maxPage

    # 获取对应产品详情
    def GetMessagesDetail(self, info, pageNum=1, proxy=False):
        try:
            factory_link = str(info.get('factory_link')).format(pageNum)
            factory_hashkey = info.get('factory_hashkey')
            print(factory_link)
            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url=factory_link, headers=self.factory_headers, proxies=pro, timeout=5,
                                         verify=False)
                else:
                    resp = requests.post(url=factory_link, headers=self.factory_headers, timeout=5, verify=False)
            else:
                resp = requests.post(url=factory_link, headers=self.factory_headers, timeout=5, verify=False)

            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                maxpage = self.ParseMessagesDetail(info.get('factory_link'), pageNum, factory_hashkey, resp.text)
                if pageNum <= maxpage:
                    return self.GetMessagesDetail(info, pageNum + 1, proxy)
                else:
                    self.factory_coll.update_one({'factory_link': info.get('factory_link')}, {'$set': {'status': 1}},
                                                 upsert=True)
                    print('已经最大页')
            else:
                print(resp.status_code)
        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetMessagesDetail(info, pageNum, proxy)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)

        time.sleep(2)

    # 解析对应产品详情
    def ParseMessagesDetail(self, factory_link, pageNum, factory_hashkey, Html):
        soup = BeautifulSoup(Html, 'lxml')
        try:
            # 第一页时更新简称 详细描述
            if pageNum == 1:
                try:
                    h2 = soup.find('div', {'class': 'producerIntroduceR'}).find('h2').get_text().replace('\n',
                                                                                                         '').replace(
                        '\t', '').replace('\r', '').strip()
                    desc = soup.find('div', {'class': 'producerIntroduceR'}).find('p').get_text().replace('\n',
                                                                                                          '').replace(
                        '\t', '').replace('\r', '').strip()
                    self.factory_coll.update_one({'factory_link': factory_link}, {'$set': {'厂家名称': h2, '厂家简介': desc}},
                                                 upsert=True)
                except:
                    pass

            # 获取所有产品
            try:
                if soup.find('div', {'class': 'producerContentL'}):
                    for div in soup.find('div', {'class': 'producerContentL'}).find_all('div', {
                        'class': 'producerContentElement clearfix'}):
                        try:
                            for p in div.find('div', {'class': 'producerContentElementR'}).find_all('p'):
                                try:
                                    if '&from=producer' in p.find('a').get('href'):
                                        wuxing_link = p.find('a').get('href')
                                        if not str(wuxing_link).startswith('https://www.antsoo.com'):
                                            wuxing_link = 'https://www.antsoo.com' + wuxing_link

                                        wuxing_hashkey = hashlib.md5(str(wuxing_link).encode("utf8")).hexdigest()

                                        self.wuxing_coll.update_one({'wuxing_link': wuxing_link}, {
                                            '$set': {'wuxing_hashkey': wuxing_hashkey, 'wuxing_link': wuxing_link,
                                                     'factory_hashkey': factory_hashkey}}, upsert=True)
                                except Exception as error:
                                    logger.warning('解析产品物性表出错 -- {}'.format(p))
                                    logger.warning(error)

                                try:
                                    if 'producersupplier?' in p.find('a').get('href'):
                                        supplier_link = p.find('a').get('href')
                                        if not str(supplier_link).startswith('https://www.antsoo.com'):
                                            supplier_link = 'https://www.antsoo.com' + supplier_link

                                        supplier_hashkey = hashlib.md5(str(supplier_link).encode("utf8")).hexdigest()

                                        self.supplier_coll.update_one({'supplier_link': supplier_link}, {
                                            '$set': {'supplier_hashkey': supplier_hashkey,
                                                     'supplier_link': supplier_link,
                                                     'factory_hashkey': factory_hashkey}}, upsert=True)
                                except Exception as error:
                                    logger.warning('解析产品供应商出错 -- {}'.format(p))
                                    logger.warning(error)
                        except Exception as error:
                            logger.warning('获取该公司产品列表第{}页出错'.format(pageNum))
                            logger.warning(error)
                else:
                    print('没有产品表')
            except:
                pass

            # 最大页
            try:
                maxPage = re.findall('共(\d+)条，(\d+)页', Html, re.S)
                if maxPage:
                    maxPage = int(maxPage[0][1])
                else:
                    maxPage = 0
            except:
                maxPage = 0

        except Exception as error:
            logger.warning(error)

        return maxPage

    # 多进程获取产品数据
    def MessagesDetailThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=5)

        logger.warning(self.factory_coll.find({}, no_cursor_timeout=True).count())
        for info in self.factory_coll.find({}, no_cursor_timeout=True):
            if Async:
                out = pool.apply_async(func=self.GetMessagesDetail, args=(info, 1, proxy,))  # 异步
            else:
                out = pool.apply(func=self.GetMessagesDetail, args=(info, 1, proxy,))  # 同步
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

    # 获取物性数据
    def GetWuxingDetail(self, info, proxy=False):
        try:
            materialsId = info.get('wuxing_link').split('?id=')[1].split('&')[0]

            jsonData = {
                "userId": 115226,
                "materialsId": materialsId
            }

            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url=self.wuxing_api, headers=self.wuxing_headers, proxies=pro,
                                         data=json.dumps(jsonData), timeout=5, verify=False)
                else:
                    resp = requests.post(url=self.wuxing_api, headers=self.wuxing_headers, data=json.dumps(jsonData),
                                         timeout=5, verify=False)
            else:
                resp = requests.post(url=self.wuxing_api, headers=self.wuxing_headers, data=json.dumps(jsonData),
                                     timeout=5, verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data = self.ParseWuxingDetail(resp.json())
                if data:
                    data.update({
                        'status': 1
                    })
                    self.wuxing_coll.update_one({'wuxing_hashkey': info['wuxing_hashkey']}, {'$set': data}, upsert=True)
        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetWuxingDetail(info, proxy)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)

    # 解析对应物性详情
    @staticmethod
    def ParseWuxingDetail(htmlJson):
        data = {}
        try:
            if htmlJson.get('body'):
                if htmlJson.get('body').get('materialsPropDetailDto'):
                    data.update({
                        'pdf_link': htmlJson.get('body').get('materialsPropDetailDto').get('isoPdfUrl'),
                        '牌号': htmlJson.get('body').get('materialsPropDetailDto').get('productSign'),
                        '品名': htmlJson.get('body').get('materialsPropDetailDto').get('materialsCategory'),
                        '厂商': htmlJson.get('body').get('materialsPropDetailDto').get('producer')
                    })
                    if htmlJson.get('body').get('materialsPropDetailDto').get('nameValueDtoList'):
                        for msg in htmlJson.get('body').get('materialsPropDetailDto').get('nameValueDtoList'):
                            data.update({
                                msg.get('name'): msg.get('value')
                            })
                if htmlJson.get('body').get('materialsPropDetailDto').get('performanceCategoryDtoList'):
                    performance_list = []
                    for Category in htmlJson.get('body').get('materialsPropDetailDto').get(
                            'performanceCategoryDtoList'):
                        if Category.get('materialsPropDtoList'):
                            materials_list = []
                            for materials in Category.get('materialsPropDtoList')[1:]:
                                materials_list.append({
                                    '测试数据': materials.get('performanceName'),
                                    '测试条件': materials.get('detectionCondition'),
                                    '测试标准': materials.get('seriesName'),
                                    '测试结果': materials.get('testData')
                                })
                            if materials_list:
                                performance_list.append({
                                    Category.get('performanceCategoryName'): materials_list
                                })
                    if performance_list:
                        data.update({
                            '物性参数': performance_list
                        })
        except Exception as error:
            logger.warning(error)
        return data

    # 多进程获取物性数据
    def WuxingDetailThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=4)

        logger.warning(self.wuxing_coll.find({'status': None}, no_cursor_timeout=True).count())
        for info in self.wuxing_coll.find({'status': None}, no_cursor_timeout=True):
            print(info)
            if Async:
                out = pool.apply_async(func=self.GetWuxingDetail, args=(info, proxy,))  # 异步
            else:
                out = pool.apply(func=self.GetWuxingDetail, args=(info, proxy,))  # 同步
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

    # 获取供应商数据
    def GetSupplierList(self, info, pageNum=1, proxy=False):
        try:
            MaterialsName = re.findall('&category=(\d+)', info.get('supplier_link'), re.S)[0]
            producerShortName = re.findall('&producer=(\d+)', info.get('supplier_link'), re.S)[0]
            model = re.findall('&model=(\d+)', info.get('supplier_link'), re.S)[0]
            jsonData = {
                'dbIndustryCategoryMaterialsName': MaterialsName,
                'model': model,
                'pIndex': pageNum,
                'pSize': 10,
                'producerShortName': producerShortName
            }

            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url=self.supplier_api, headers=self.supplier_headers, proxies=pro,
                                         data=json.dumps(jsonData), timeout=5, verify=False)
                else:
                    resp = requests.post(url=self.supplier_api, headers=self.supplier_headers,
                                         data=json.dumps(jsonData),
                                         timeout=5, verify=False)
            else:
                resp = requests.post(url=self.supplier_api, headers=self.supplier_headers, data=json.dumps(jsonData),
                                     timeout=5, verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data_list, maxpage = self.ParseSupplierList(resp.json())
                if data_list:
                    for data in data_list:
                        print(data)
                        data.update({
                            'supplier_hashkey': info['supplier_hashkey']
                        })
                        self.supplier_detail_coll.update_one({'supplier_detail_link': data['supplier_detail_link']},
                                                             {'$set': data}, upsert=True)
                if pageNum <= maxpage:
                    return self.GetSupplierList(info, pageNum + 1, proxy)
                else:
                    self.supplier_coll.update_one({'supplier_link': info['supplier_link']}, {'$set': {'status': 1}},
                                                  upsert=True)
        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetSupplierList(info, pageNum, proxy)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)

    # 解析对应供应商详情
    @staticmethod
    def ParseSupplierList(htmlJson):
        dataList = []
        try:
            if htmlJson.get('body'):
                pm = htmlJson.get('body').get('tdk').get('title').split('-')[0]
                ph = htmlJson.get('body').get('tdk').get('title').split('-')[2]
                cj = htmlJson.get('body').get('tdk').get('title').split('-')[1]
                for supplier in htmlJson.get('body').get('supplierList'):
                    dataList.append({
                        '供应商名称': supplier.get('enterpriseName'),
                        '代理': supplier.get('isAgency'),
                        '入驻': supplier.get('isAdmin'),
                        '旗舰店': supplier.get('isMember'),
                        '品名': pm,
                        '牌号': ph,
                        '厂家': cj,
                        '价格': supplier.get('price'),
                        '发货地': supplier.get('deliveryPlace'),
                        '报价时间': supplier.get('priceTime'),
                        'supplier_detail_link': 'https://www.antsoo.com/enterprise/store/storeproductdetail?id={0}&productid={1}&to={2}'.format(
                            supplier.get('enterpriseId'), supplier.get('productId'),
                            supplier.get('enterprisePowerType'))
                    })
        except Exception as error:
            logger.warning(error)

        # 最大页
        try:
            maxPage = htmlJson.get('page').get('pages')
        except:
            maxPage = 0

        return dataList, maxPage

    # 多线程获取供应商数据
    def SupplierListThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=4)

        logger.warning(self.supplier_coll.find({}, no_cursor_timeout=True).count())
        for info in self.supplier_coll.find({}, no_cursor_timeout=True):
            print(info)
            if Async:
                out = pool.apply_async(func=self.GetSupplierList, args=(info, 1, proxy,))  # 异步
            else:
                out = pool.apply(func=self.GetSupplierList, args=(info, 1, proxy,))  # 同步
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

    # 获取pdf信息
    def GetPDF(self):
        for info in self.wuxing_coll.find({'$nor': [{'pdf_link': ''}], 'status': 1}):
            self.DownLoadPDF(info)
            # break

    # 下载pdf
    def DownLoadPDF(self, info, retry=1):
        filePath = self.downloadPath + '/ansou_spk/'
        if not os.path.exists(filePath):
            os.makedirs(filePath)

        # pdf
        try:
            pdfLink = info.get('pdf_link') + '?response-content-type=application/octet-stream'
            resp = requests.get(pdfLink, headers=self.downloadPdfHeaders, verify=False)
            # 存储到本地
            if resp.content:
                fp = filePath + '{}.pdf'.format(info['wuxing_hashkey'])
                print(fp)
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()

                self.wuxing_coll.update_one({'wuxing_hashkey': info['wuxing_hashkey']}, {'$set': {'status': 2}},
                                            upsert=True)
        except requests.exceptions.ConnectionError:
            if retry < 3:
                print('网络问题，重试中...')
                return self.DownLoadPDF(info, retry + 1)
        except Exception as error:
            logger.warning(error)


def run():
    ansou = AnsouSPK()

    # 获取所有厂家信息
    ansou.GetChangjiaMessages()

    # 多线程获取厂家详情
    ansou.MessagesDetailThread()

    # 多线程获取物性数据
    ansou.WuxingDetailThread()

    # 多线程获取供应商信息
    ansou.SupplierListThread()

    # 下载物性pdf
    ansou.GetPDF()


if __name__ == '__main__':
    run()
