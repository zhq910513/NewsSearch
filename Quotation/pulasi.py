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
from os import path

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/pulasi.log'))
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


class PuLaSi:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        proxydb = conf.get("Mongo", "PROXY")

        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))

        proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        # proxyclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]

        self.category_coll = client[datadb]['pls_category']
        self.supplier_coll = client[datadb]['pls_suppliers']
        self.supplier_detail_coll = client[datadb]['pls_supplier_detail']

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

    # 获取所有分类
    def GetAllCategory(self, proxy=False):
        try:
            link = 'https://www.plasway.com/price/74'
            headers = {
                'authority': 'www.plasway.com',
                'method': 'GET',
                'path': '/price/74',
                'scheme': 'https',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }

            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(url=link, headers=headers, proxies=pro, timeout=5, verify=False)
                else:
                    resp = requests.get(url=link, headers=headers, timeout=5, verify=False)
            else:
                resp = requests.get(url=link, headers=headers, timeout=5, verify=False)

            resp.encoding = 'gbk'
            if resp.status_code == 200:
                data_list = self.ParseAllCategory(resp.text)
                if data_list:
                    for data in data_list:
                        self.category_coll.update_one({'link': data.get('link')}, {'$set': data}, upsert=True)
            else:
                print(resp.status_code)
        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetAllCategory(proxy)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)

    # 解析所有分类
    @staticmethod
    def ParseAllCategory(Html):
        dataList = []
        soup = BeautifulSoup(Html, 'lxml')
        try:
            # 获取分类
            if soup.find('div', {'class': 'nav-right-padding'}):
                for li in soup.find('div', {'class': 'nav-right-padding'}).find_all('li', {'class': 'prop-attrs'}):
                    try:
                        if '通用塑料' in li.find('div', {'class': 'partyLeft'}).get_text().strip():
                            dataList = [
                                {'Type': a.get_text().strip(), 'link': 'https://www.plasway.com' + a.get('href')} for a
                                in li.find('div', {'class': 'partyRight'}).find_all('a')]
                    except:
                        pass
            else:
                logger.warning('没有分类信息表')
        except Exception as error:
            logger.warning(error)
        return dataList

    # 获取分类下供应商列表
    def GetSupplierList(self, info, proxy=False):
        Type = info.get('Type')
        for o_t in ['ABS', 'EVA', 'GPPS', 'HDPE', 'HIPS', 'LDPE', 'LLDPE', 'PET', 'PP', 'PVC', 'PA6', 'PA66', 'PC']:
            if o_t in Type:
                try:
                    headers = {
                        'authority': 'www.plasway.com',
                        'method': 'GET',
                        'path': '/price/74/35',
                        'scheme': 'https',
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'cache-control': 'no-cache',
                        'pragma': 'no-cache',
                        'upgrade-insecure-requests': '1',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
                    }
                    if proxy:
                        # 获取代理
                        pro = self.GetProxy()
                        if pro:
                            resp = requests.get(url=info.get('link'), headers=headers, proxies=pro, timeout=5,
                                                 verify=False)
                        else:
                            resp = requests.get(url=info.get('link'), headers=headers, timeout=5, verify=False)
                    else:
                        resp = requests.get(url=info.get('link'), headers=headers, timeout=5, verify=False)
                    resp.encoding = 'gbk'
                    if resp.status_code == 200:
                        data_list = self.ParseSupplierList(resp.text)
                        if data_list:
                            for data in data_list:
                                print(data)
                                data.update({'Type': Type})
                                self.supplier_coll.update_one({'supplier_link': data.get('supplier_link')}, {'$set': data}, upsert=True)
                            self.category_coll.update_one({'link': info.get('link')}, {'$set': {'status': 1}}, upsert=True)
                except requests.exceptions.ConnectionError:
                    threading.Thread(target=self.DisProxy, args=(pro,)).start()
                    print('网络问题，重试中...')
                    return self.GetSupplierList(info, proxy)
                except TimeoutError:
                    pass
                except Exception as error:
                    logger.warning(error)

                time.sleep(1)

    # 解析分类下供应商列表
    @staticmethod
    def ParseSupplierList(Html):
        dataList = []
        soup = BeautifulSoup(Html, 'lxml')
        try:
            print(soup.find('div', {'class': 'price-center-center'}))
            if soup.find('div', {'class': 'price-center-center'}):
                titles = [th.get_text() for th in soup.find('div', {'class': 'price-center-center'}).find('thead').find_all('th')]

                if soup.find('div', {'class': 'price-center-center'}).find('tbody'):
                    for tr in soup.find('div', {'class': 'price-center-center'}).find('tbody').find_all('tr'):
                        data = {}
                        try:
                            if len(titles) == len(tr.find_all('td')):
                                for i in range(len(titles)):
                                    key = titles[i]
                                    value = tr.find_all('td')[i].get_text().strip().replace('\n', '').replace('\t',
                                                                                                              '').replace(
                                        '\r', '')
                                    if key == '推荐供应商':
                                        supplier_link = 'https://www.plasway.com' + tr.find_all('td')[i].find('a').get(
                                            'href')
                                        data.update({
                                            'supplier_hashkey': hashlib.md5(
                                                str(supplier_link).encode("utf8")).hexdigest(),
                                            'supplier_link': supplier_link
                                        })
                                    data.update({
                                        key: value
                                    })
                        except:
                            pass
                        if data:
                            dataList.append(data)
        except Exception as error:
            logger.warning(error)

        return dataList

    # 多线程获取供应商数据
    def SupplierListThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=4)

        logger.warning(self.category_coll.find({"Type" : "AS(SAN)"}, no_cursor_timeout=True).count())
        for info in self.category_coll.find({"Type" : "AS(SAN)"}, no_cursor_timeout=True):
            print(info)
            if Async:
                out = pool.apply_async(func=self.GetSupplierList, args=(info, proxy,))  # 异步
            else:
                out = pool.apply(func=self.GetSupplierList, args=(info, proxy,))  # 同步
            thread_list.append(out)
            break

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

    # 获取分类下供应商列表
    def GetSupplierDetail(self, info, proxy=False):
        try:
            headers = {
                'authority': 'www.plasway.com',
                'method': 'GET',
                'path': '/price/74/35',
                'scheme': 'https',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
            }
            if proxy:
                # 获取代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(url=info.get('supplier_link'), headers=headers, proxies=pro, timeout=5,
                                        verify=False)
                else:
                    resp = requests.get(url=info.get('supplier_link'), headers=headers, timeout=5, verify=False)
            else:
                resp = requests.get(url=info.get('supplier_link'), headers=headers, timeout=5, verify=False)
            resp.encoding = 'gbk'
            if resp.status_code == 200:
                data_list = self.ParseSupplierDetail(resp.text)
                if data_list:
                    if data_list == 'change-ip':
                        threading.Thread(target=self.DisProxy, args=(pro,)).start()
                        print('网络问题，重试中...')
                        return self.GetSupplierDetail(info, proxy)
                    for data in data_list:
                        print(data)
                        data.update({
                            'supplier_hashkey': info.get('supplier_hashkey'),
                        })
                        self.supplier_detail_coll.update_one({'hashkey': data.get('hashkey')}, {'$set': data},
                                                             upsert=True)
                    self.supplier_coll.update_one({'supplier_hashkey': info.get('supplier_hashkey')},
                                                  {'$set': {'status': 1}}, upsert=True)
        except requests.exceptions.ConnectionError:
            threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetSupplierDetail(info, proxy)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)

        time.sleep(1)

    # 解析分类下供应商列表
    @staticmethod
    def ParseSupplierDetail(Html):
        dataList = []
        soup = BeautifulSoup(Html, 'lxml')

        search_time = time.strftime("%Y-%m-%d", time.localtime(time.time()))
        hash_time = str(time.strftime("%Y-%m-%d %H", time.localtime(time.time())))

        try:
            if soup.find('table', {'class': 'report'}):
                titles = [th.get_text().strip().replace('\n', '').replace('\t', '').replace('\r', '') for th in
                          soup.find('table', {'class': 'report'}).find('thead').find_all('th')]
                # print(titles)

                if soup.find('table', {'class': 'report'}).find('tbody'):
                    for tr in soup.find('table', {'class': 'report'}).find('tbody').find_all('tr'):
                        if '暂无数据' in str(tr):
                            return 'change-ip'

                        data = {'采集时间': search_time}
                        try:
                            if len(titles) == len(tr.find_all('td')):
                                for i in range(len(titles)):
                                    try:
                                        key = titles[i]
                                        if key not in ['数量', '包装', '试样料', '总销量', '商务洽谈']:
                                            if '供应商' in key:
                                                value = tr.find_all('td')[i].find('a').get_text()
                                                detail_info = {}
                                                try:
                                                    member_year = tr.find_all('td')[i].find('div', {
                                                        'class': 'member_year'}).get('title')
                                                    detail_info.update({'交易会员': member_year})
                                                except:
                                                    pass

                                                for li in tr.find_all('li'):
                                                    try:
                                                        key = li.find('div', {
                                                            'class': 'left-side'}).get_text().strip().replace('\n',
                                                                                                              '').replace(
                                                            '\t', '').replace('\r', '').replace('\xa0', '').replace(' ',
                                                                                                                    '')

                                                        if '联系人' in key:
                                                            key = '联系人'
                                                            value = li.find_all('div', {'class': 'left-side'})[
                                                                -1].get_text().strip().replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                                                '\r', '').replace('\xa0', '').replace(' ', '').replace(
                                                                '商务洽谈', '')
                                                        elif '联系方式' in key:
                                                            key = '联系方式'
                                                            value = li.find_all('div', {'class': 'left-side'})[
                                                                -1].get_text().strip().replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                                                '\r', '').replace('\xa0', '').replace(' ', '')
                                                        elif '地址' in key:
                                                            key = '地址'
                                                            value = li.find_all('div', {'class': 'left-side'})[
                                                                -1].get_text().strip().replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                                                '\r', '').replace('\xa0', '').replace(' ', '').split(
                                                                '高价回收')[0]
                                                        elif '成交量' in key:
                                                            value = key.split('：')[1]
                                                            key = '成交量'
                                                        elif '最近一次' in key:
                                                            value = key.split('：')[1]
                                                            key = '最近一次成交'
                                                        else:
                                                            value = li.find('a').get_text().strip().replace('\n',
                                                                                                            '').replace(
                                                                '\t', '').replace('\r', '').replace('\xa0', '').replace(
                                                                ' ', '')

                                                        detail_info.update({
                                                            key: value
                                                        })
                                                    except:
                                                        pass
                                                data.update({
                                                    'detail_info': detail_info
                                                })
                                            elif '发布时间' in key:
                                                value = tr.find_all('td')[i].get_text().strip().replace('\n',
                                                                                                        '').replace(
                                                    '\t', '').replace('\r', '')
                                                if '分钟' in value and '小时' not in value and '天' not in value:
                                                    uploadTime = time.time() - (int(value.split('分')[0]) * 60)
                                                    value = time.strftime("%Y-%m-%d %H:%M:%S",
                                                                          time.localtime(uploadTime))
                                                elif '小时' in value and '分钟' not in value and '天' not in value:
                                                    uploadTime = time.time() - (int(value.split('小时')[0]) * 60 * 60)
                                                    value = time.strftime("%Y-%m-%d %H:%M:%S",
                                                                          time.localtime(uploadTime))
                                                elif '天' in value and '分钟' not in value and '小时' not in value:
                                                    uploadTime = time.time() - (int(value.split('天')[0]) * 60 * 60 * 24)
                                                    value = time.strftime("%Y-%m-%d %H:%M:%S",
                                                                          time.localtime(uploadTime))

                                                else:
                                                    value = None
                                            else:
                                                value = tr.find_all('td')[i].get_text().strip().replace('\n',
                                                                                                        '').replace(
                                                    '\t', '').replace('\r', '')
                                                if '详情' in value:
                                                    value = 'https://www.plasway.com' + tr.find_all('td')[i].find(
                                                        'a').get('href')
                                            data.update({
                                                key: value
                                            })
                                    except:
                                        pass
                        except:
                            pass
                        if data:
                            hashkey = hashlib.md5(str(hash_time + data.get('操作')).encode("utf8")).hexdigest()
                            data.update({'hashkey': hashkey})
                            dataList.append(data)
        except Exception as error:
            logger.warning(error)

        return dataList

    # 多线程获取供应商数据
    def SupplierDetailThread(self, proxy=False, remove_bad=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=4)

        logger.warning(self.supplier_coll.find({}, no_cursor_timeout=True).count())
        for info in self.supplier_coll.find({}, no_cursor_timeout=True):
            print(info)
            if Async:
                out = pool.apply_async(func=self.GetSupplierDetail, args=(info, proxy,))  # 异步
            else:
                out = pool.apply(func=self.GetSupplierDetail, args=(info, proxy,))  # 同步
            thread_list.append(out)
            break

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


def run():
    pls = PuLaSi()

    # 获取所有厂家信息
    # pls.GetAllCategory(proxy=True)

    # 多线程获取供应商列表
    pls.SupplierListThread(proxy=False)

    # 多线程获取供应商详情
    # pls.SupplierDetailThread(proxy=True)


if __name__ == '__main__':
    run()
