#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import random
import sys

sys.path.append("../")
import configparser
import json
import logging
import pprint
import time
import hashlib
import pandas as pd

import os
from os import path
from urllib import parse
import datetime
from datetime import datetime as dtime

import requests
from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient
import pymysql
from pymysql.err import IntegrityError

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/jinlianchuang_xh.log'))
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


# 搜索规则 ：每次启动从数据库获取categoryUrl，结束时间自动设置为当天


class JinLianChuang:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        cookiedb = conf.get("Mongo", "COOKIE")
        proxydb = conf.get("Mongo", "PROXY")

        # client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=datadb))

        # cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        cookieclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']

        # proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        proxyclient = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]

        self.category_coll = client[datadb]['jlc_xh_category']
        self.categoryData_coll = client[datadb]['jlc_xh_categoryData']
        self.downloadDetail_coll = client[datadb]['jlc_xh_downloadData']

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

        # 请求头信息
        self.userAgent = UserAgent().random
        self.categoryHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'jiag.315i.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.categoryDataHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'jiag.315i.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.downloadDetailUrl = 'https://dc.oilchem.net/price_search/doExportZip.htm?startDate=20190101&endDate=20200915&indexType=0&businessInformation=%5B%7B%22businessType%22%3A%22{0}%22%2C%22varietiesId%22%3A%22{1}%22%2C%22businessIdList%22%3A%5B{2}%5D%7D%5D&fileName={3}.xlsx'
        self.downloadDetailHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '131',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'jiag.315i.com',
            'Origin': 'http://jiag.315i.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
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

    # 获取所有层级类目
    def GetCategory(self, proxy=False):
        try:
            if proxy:
                # 启用代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(url='http://jiag.315i.com/price/newindex/004', headers=self.categoryHeaders,
                                        proxies=pro, timeout=5, verify=False)
                else:
                    resp = requests.get(url='http://jiag.315i.com/price/newindex/004', headers=self.categoryHeaders,
                                        timeout=5, verify=False)
            else:
                resp = requests.get(url='http://jiag.315i.com/price/newindex/004', headers=self.categoryHeaders,
                                    timeout=5, verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data = self.ParseCategory(resp.text)
                if data:
                    for info in data:
                        print(info)
                        for timeType in [0, 1, 2]:
                            self.ParseAreaCategory(proxy, info, timeType)
                    print('jlc_xh 获取所有层级类目--完成')
        except requests.exceptions.ConnectionError:
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetCategory(proxy)
        except TimeoutError:
            pass
        except Exception as error:
            logger.warning(error)
            return

    # 解析所有层级类目
    @staticmethod
    def ParseCategory(html):
        soup = BeautifulSoup(html, 'lxml')
        dataList = []

        categorys = soup.find('div', {'class': 'sidebar fl'}).find_all('h3', {'class': 'cur'})
        items = soup.find('div', {'class': 'sidebar fl'}).find_all('ul', {'class': 'sufir'})
        categoryNum = len(categorys)

        for num in range(categoryNum):
            categoryOne = categorys[num].get_text().strip()
            for two in items[num].find_all('li', {'name': 'li_Product'}):
                categoryTwo = two.find('a').get_text().strip()
                for three in two.find('ul', {'class': 'sec dis_no'}).find_all('li'):
                    try:
                        categoryThree = three.find('a').get_text().strip()
                        try:
                            for four in three.find('ol', {'class': 'ol_list'}).find_all('li'):
                                categoryFour = four.find('a').get_text().strip()
                                link = 'http://jiag.315i.com' + four.find('a').get('href')
                                dataList.append({
                                    'categoryOne': categoryOne,
                                    'categoryTwo': categoryTwo,
                                    'categoryThree': categoryThree,
                                    'categoryFour': categoryFour,
                                    'link': link
                                })
                        except:
                            link = 'http://jiag.315i.com' + three.find('a').get('href')
                            dataList.append({
                                'categoryOne': categoryOne,
                                'categoryTwo': categoryTwo,
                                'categoryThree': categoryThree,
                                'link': link
                            })
                    except:
                        pass

        if dataList:
            return dataList

    # 获取分区域下的类目
    def ParseAreaCategory(self, proxy, info, timeType):
        try:
            if timeType == 0:
                Type = '日报价'
            elif timeType == 1:
                Type = '周均价'
            elif timeType == 2:
                Type = '月均价'
            else:
                Type = ''
            info.update({
                'Type': Type,
                'timeType': timeType
            })

            link = info.get('link').split('timeType=')[0] + 'timeType={}'.format(timeType)
            info['link'] = link

            if proxy:
                # 启用代理
                pro = self.GetProxy()
                resp = requests.get(url=link, headers=self.categoryHeaders, proxies=pro, timeout=5, verify=False)
            else:
                resp = requests.get(url=link, headers=self.categoryHeaders, timeout=5, verify=False)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                areaList = []

                try:
                    area = soup.find('div', {'class': 'areamain typearea1'}).find_all('li')
                    if area:
                        for li in area:
                            if '日报价' in li.find('a').get_text() or '周报价' in li.find('a').get_text() or '月报价' in li.find(
                                    'a').get_text():
                                pass
                            else:
                                areaList.append((li.get('id').split('_')[1],
                                                 li.find('a').get_text().strip().replace('\n', '').replace('\t',
                                                                                                           '').replace(
                                                     '\r',
                                                     '')))
                    else:
                        pass
                except:
                    pass

                if areaList:
                    for areaMsg in areaList:
                        newLink = link.split('dateColumnClassId')[
                                      0] + f'dateColumnClassId={areaMsg[0]}&timeType={timeType}'
                        info['link'] = newLink
                        info['dateColumnClassId'] = areaMsg[0]
                        info['ColumnClassIdText'] = areaMsg[1]

                        # 插入数据
                        print(info)
                        self.category_coll.update_one({'link': newLink}, {'$set': info}, upsert=True)
                else:
                    try:
                        info['dateColumnClassId'] = info.get('link').split('dateColumnClassId=')[1].split('&')[0]
                    except:
                        info['dateColumnClassId'] = ''

                    if info.get('categoryFour'):
                        info['ColumnClassIdText'] = info.get('categoryFour')
                    elif info.get('categoryThree'):
                        info['ColumnClassIdText'] = info.get('categoryThree')
                    elif info.get('categoryTwo'):
                        info['ColumnClassIdText'] = info.get('categoryTwo')
                    else:
                        info['ColumnClassIdText'] = ''

                    # 插入数据
                    print(info)
                    self.category_coll.update_one({'link': link}, {'$set': info}, upsert=True)
            else:
                try:
                    info['dateColumnClassId'] = info.get('link').split('dateColumnClassId=')[1].split('&')[0]
                except:
                    info['dateColumnClassId'] = ''

                if info.get('categoryFour'):
                    info['ColumnClassIdText'] = info.get('categoryFour')
                elif info.get('categoryThree'):
                    info['ColumnClassIdText'] = info.get('categoryThree')
                elif info.get('categoryTwo'):
                    info['ColumnClassIdText'] = info.get('categoryTwo')
                else:
                    info['ColumnClassIdText'] = ''

                # 插入数据
                print(info)
                self.category_coll.update_one({'link': link}, {'$set': info}, upsert=True)
        except requests.exceptions.ConnectionError:
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetCategory(proxy)
        except Exception as error:
            logger.warning(error)

    """
        获取详细产品数据
    """

    # 获取层级类目下的细分区域 / 产品链接
    def GetCategoryData(self, info, proxy):
        Type = info.get('Type')
        timeType = info.get('timeType')

        try:
            if proxy:
                # 启用代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(url=info.get('link'), headers=self.categoryDataHeaders, proxies=pro, timeout=5,
                                        verify=False)
                else:
                    resp = requests.get(url=info.get('link'), headers=self.categoryDataHeaders, timeout=5, verify=False)
            else:
                resp = requests.get(url=info.get('link'), headers=self.categoryDataHeaders, timeout=5, verify=False)
            if resp.status_code == 200:
                data = self.ParseCategoryData(resp.text)
                if data:
                    for param in data:
                        info.update(param)
                        info.update({
                            'hashKey': hashlib.md5((str(info.get('link')) + str(Type) + str(
                                param.get('columnid')) + str(param.get('itemIdStr')) + str(
                                param.get('ColumnClassIdText'))).encode("utf8")).hexdigest(),  # 数据唯一索引
                            'timeType': timeType
                        })

                        try:
                            del info['_id']
                        except:
                            pass
                        try:
                            del info['status']
                        except:
                            pass

                        # 插入数据
                        print(info)
                        self.categoryData_coll.update_one({'hashKey': info['hashKey']}, {'$set': info}, upsert=True)

                    # 标记已用数据
                    self.category_coll.update_one({'link': info.get('link')}, {'$set': {'status': 1}}, upsert=True)
                else:
                    self.category_coll.update_one({'link': info.get('link')}, {'$set': {'status': 404}}, upsert=True)
            else:
                self.category_coll.update_one({'link': info.get('link')}, {'$set': {'status': 400}}, upsert=True)
        except requests.exceptions.ConnectionError:
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.GetCategoryData(info.get('link'), proxy)
        except TimeoutError:
            logger.warning(info.get('link'))
        except Exception as error:
            logger.warning(error)
            return

        # 随机休眠
        time.sleep(random.uniform(1, 3))

    # 解析层级类目下的细分区域/产品链接
    @staticmethod
    def ParseCategoryData(html):
        soup = BeautifulSoup(html, 'lxml')
        dataList = []

        # columnid
        try:
            columnid = soup.find('input', {'value': '查看历史数据'}).get('onclick').split("'")[1]
        except:
            columnid = None

        # itemIdStr
        if columnid:
            try:
                titleList = [th.get_text().strip().replace('\n', '').replace('\t', '').replace('\r', '').replace(' ',
                                                                                                                 '') for
                             th in soup.find_all('th')][1:]
                titles = []
                for i in titleList:
                    if i not in titles and i != '选择' and '-' not in i:
                        titles.append(i)
                    else:
                        pass
                # print(titles)

                for tr in soup.find('div', {'class': 'main_rb mart1 tablemain'}).find_all('tr'):
                    try:
                        itemIdStr = tr.find('input').get('value')

                        values = [td.get_text().strip().replace('\n', '').replace('\t', '').replace('\r', '').replace(
                            ' ', '') for td in tr.find_all('td')][1:]
                        # print(values)

                        data = {}
                        for j in range(len(titles) - 3):
                            data.update({
                                titles[j]: values[j]
                            })
                        if data:
                            data.update({
                                'columnid': columnid,
                                'itemIdStr': itemIdStr,
                                titles[-2]: values[-2],
                                titles[-1]: values[-1]
                            })
                        dataList.append(data)
                    except:
                        pass
            except Exception as error:
                logger.warning(error)

        if dataList:
            return dataList

    """
        获取历史数据
    """

    # 通过详细产品链接下载历史数据， 起始时间为2019.01.01   结束时间为至今
    def DownloadHistoryData(self, info, proxy, history):
        print(info)
        columnid = info.get('columnid')
        itemIdStr = info.get('itemIdStr')
        endDate = str(time.strftime("%Y-%m-%d", time.localtime(time.time())))
        timeType = info.get('timeType')

        filePath = self.downloadPath + '/jlc/{}/'.format(timeType)
        if not os.path.exists(filePath):
            os.makedirs(filePath)

        fp = filePath + '{}.xls'.format(info['hashKey'])
        print(fp)

        url = 'http://jiag.315i.com/price/historyExport'

        formData = {
            'itemIdStr': itemIdStr,
            'pageIndex': 1,
            'endDate': endDate,
            'columnid': columnid,
            'timeType': timeType,
            'startDate': '2019-01-01'
        }

        link = f'http://jiag.315i.com/price/historyData?itemIdStr={itemIdStr}&startDate=2019-01-01&endDate={endDate}&columnid={columnid}&timeType={timeType}'
        print(link)

        data = parse.urlencode(formData)
        self.downloadDetailHeaders.update({
            'Cookie': self.cookie_coll.find_one({'name': 'jlc_xh_downloadDetail'}).get('cookie'),
            'Referer': info['link']
        })

        try:
            if proxy:
                # 启用代理
                pro = self.GetProxy()
                if pro:
                    resp = requests.post(url, headers=self.downloadDetailHeaders, proxies=pro, data=data, timeout=5,
                                         verify=False)
                else:
                    resp = requests.post(url, headers=self.downloadDetailHeaders, data=data, timeout=5, verify=False)
            else:
                resp = requests.post(url, headers=self.downloadDetailHeaders, data=data, timeout=5, verify=False)

            # 存储到本地
            if resp.content:
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()

                # 写入本地
                self.GetDataFromExcel(link, fp, info, history)

                # 标记数据库已下载文件
                self.categoryData_coll.update_one({"hashKey": info['hashKey']}, {'$set': {'status': 1}}, upsert=True)
            else:
                print('没有数据, 无栏目权限')
                self.categoryData_coll.update_one({"hashKey": info['hashKey']}, {'$set': {'status': 400}}, upsert=True)
        except requests.exceptions.ConnectionError:
            # threading.Thread(target=self.DisProxy, args=(pro,)).start()
            print('网络问题，重试中...')
            return self.DownloadHistoryData(info, proxy, history)
        except TimeoutError:
            logger.warning(url)
        except Exception as error:
            logger.warning(error)
            return

        # 随机休眠
        if not history:
            time.sleep(random.uniform(1, 1.5))

    # 读取 excel 表格数据
    def GetDataFromExcel(self, link, fp, info, history):
        dataFrame = pd.read_excel(fp, header=None)
        detailData = dataFrame.to_dict(orient='index')

        conn = self.MySql()

        if detailData:
            dumpsData = json.dumps(detailData)
            keyList = list(list(json.loads(dumpsData).values())[1].values())
            if history:
                for value in list(json.loads(dumpsData).values())[2:-1]:
                    self.FormatData(link, info, keyList, value, conn)
            else:
                for value in list(json.loads(dumpsData).values())[-5:-1]:
                    self.FormatData(link, info, keyList, value, conn)
        else:
            pass

        # 关闭MySQL连接
        conn.cursor().close()

    # 格式化数据
    def FormatData(self, link, info, keyList, data, conn):
        try:
            try:
                dt = data.get(str(keyList.index('日期')))  # 数据日期
                if isinstance(dt, str):
                    dt = dt.replace('-', '').replace('至', '-')
                    if dt == '日期' or str(dt) == 'nan':
                        return
                else:
                    return
            except ValueError:
                dt = ''
            except Exception as error:
                logger.warning(error)
                dt = ''

            hashKey = hashlib.md5(str(info.get('hashKey') + dt).encode("utf8")).hexdigest()  # 数据唯一索引
            dt_type = int(info.get('timeType')) + 1  # 报价类型(1-每日报价，2-周均价，3-月均价，4-季均价，5-年均价）
            prod_quote_type_name = info.get('categoryThree')  # 报价名称(如国际市场报价，国内市场报价，厂家报价等)
            prod_name = info.get('产品名称')  # 产品名称
            prod_factory = info.get('生产企业')  # 生产企业
            prod_sales_area = info.get('销售地区')  # 销售地区(如ABS/PS国内市场报价,有销售地区字段)
            prod_area = info.get('地区')  # 地区
            if not prod_area and info.get('categoryThree') != info.get('ColumnClassIdText'):
                prod_area = info.get('ColumnClassIdText')
            else:
                prod_area = ''
            prod_market = info.get('产品市场')  # 产品市场
            prod_type = info.get('产品种类')  # 产品种类(如:膜料)
            prod_brand = info.get('牌号')  # 牌号
            prod_color = info.get('颜色')  # 颜色
            prod_thicknesses = info.get('厚度')  # 厚度
            prod_level = info.get('级别')  # 级别
            prod_process = info.get('工艺')  # 工艺
            prod_unit = info.get('单位')  # 单位
            prod_specifications = info.get('产品规格')  # 产品规格
            prod_warehouse = info.get('仓库名称')  # 仓库名称
            prod_price_type = info.get('价格类型')  # 价格类型
            try:
                prod_lowest_price = data.get(str(keyList.index('最低价')))  # 最低价
                if prod_lowest_price:
                    prod_lowest_price = str(prod_lowest_price).replace('/', '').replace('nan', '').replace('-',
                                                                                                           '').replace(
                        'None', '').replace('none', '').replace('Null', '').replace('null', '')
                    if prod_lowest_price:
                        try:
                            prod_lowest_price = round(float(prod_lowest_price), 2)
                        except:
                            prod_lowest_price = 0.00
            except ValueError:
                prod_lowest_price = 0.00
            except Exception as error:
                logger.warning(error)
                prod_lowest_price = 0.00

            try:
                prod_higest_price = data.get(str(keyList.index('最高价')))  # 最高价
                if prod_higest_price:
                    prod_higest_price = str(prod_higest_price).replace('/', '').replace('nan', '').replace('-',
                                                                                                           '').replace(
                        'None', '').replace('none', '').replace('Null', '').replace('null', '')
                    if prod_higest_price:
                        try:
                            prod_higest_price = round(float(prod_higest_price), 2)
                        except:
                            prod_higest_price = 0.00
            except ValueError:
                prod_higest_price = 0.00
            except Exception as error:
                logger.warning(error)
                prod_higest_price = 0.00

            try:
                prod_average_price = data.get(str(keyList.index('价格')))  # 平均价(生产厂家报价)
                if prod_average_price:
                    prod_average_price = str(prod_average_price).replace('/', '').replace('nan', '').replace('-',
                                                                                                             '').replace(
                        'None', '').replace('none', '').replace('Null', '').replace('null', '')
                    if prod_average_price:
                        try:
                            prod_average_price = round(float(prod_average_price), 2)
                        except:
                            prod_average_price = 0.00
            except ValueError:
                prod_average_price = 0.00
            except Exception as error:
                logger.warning(error)
                prod_average_price = 0.00

            try:
                prod_remark = str(data.get(str(keyList.index('备注')))).replace('/', '').replace('nan', '').replace('-',
                                                                                                                  '').replace(
                    'None', '').replace('none', '').replace('Null', '').replace('null', '').replace(' ', '')  # 产品备注
                if not prod_remark:
                    prod_remark = str(info.get('备注')).replace('/', '').replace('nan', '').replace('-', '').replace(
                        'None', '').replace('none', '').replace('Null', '').replace('null', '').replace(' ', '')  # 产品备注
            except ValueError:
                prod_remark = str(info.get('备注')).replace('/', '').replace('nan', '').replace('-', '').replace('None',
                                                                                                               '').replace(
                    'none', '').replace('Null', '').replace('null', '').replace(' ', '')  # 产品备注
            except Exception as error:
                logger.warning(error)
                prod_remark = str(info.get('备注')).replace('/', '').replace('nan', '').replace('-', '').replace('None',
                                                                                                               '').replace(
                    'none', '').replace('Null', '').replace('null', '').replace(' ', '')

            prod_change_amount = 0.00  # 涨跌额
            prod_change_rate = 0.00  # 涨跌幅
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 创建日期
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))  # 更新日期
            plat_source_id = 3  # 数据来源ID(1:隆众数据 2:卓创数据 3:金联创 4:上期所 5:东方财富)
            plat_source_name = '金联创现货'  # 数据来源备注
            plat_source_url = link  # 数据来源网址

            insertSql = '''INSERT INTO jlc_price_info(hashKey, dt, dt_type, prod_quote_type_name, prod_name,
            prod_factory, prod_sales_area, prod_area, prod_market, prod_type, prod_brand, prod_color, prod_thicknesses,
            prod_level, prod_process, prod_unit, prod_remark, prod_specifications, prod_warehouse, prod_price_type, prod_lowest_price,
            prod_higest_price, prod_average_price, prod_change_amount, prod_change_rate, create_time, update_time, plat_source_id,
            plat_source_name, plat_source_url)
            VALUES('%s','%s','%d','%s','%s',
            '%s','%s','%s','%s','%s','%s','%s','%s',
            '%s','%s','%s','%s','%s','%s','%s','%f',
            '%f','%f','%f','%f','%s','%s','%d',
            '%s','%s')'''

            insertData = (
                hashKey,
                str(dt),
                int(dt_type),
                str(prod_quote_type_name) if prod_quote_type_name else '',
                str(prod_name) if prod_name else '',
                str(prod_factory) if prod_factory else '',
                str(prod_sales_area) if prod_sales_area else '',
                str(prod_area) if prod_area else '',
                str(prod_market) if prod_market else '',
                str(prod_type) if prod_type else '',
                str(prod_brand) if prod_brand else '',
                str(prod_color) if prod_color else '',
                str(prod_thicknesses) if prod_thicknesses else '',
                str(prod_level) if prod_level else '',
                str(prod_process) if prod_process else '',
                str(prod_unit) if prod_unit else '',
                str(prod_remark) if prod_remark else '',
                str(prod_specifications) if prod_specifications else '',
                str(prod_warehouse) if prod_warehouse else '',
                str(prod_price_type) if prod_price_type else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_higest_price) if prod_higest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                create_time,
                update_time,
                int(plat_source_id),
                str(plat_source_name),
                str(plat_source_url)
            )

            updateSql = "update jlc_price_info set dt='%s', dt_type='%d', prod_quote_type_name='%s', prod_name='%s'," \
                        "prod_factory='%s', prod_sales_area='%s', prod_area='%s', prod_market='%s', prod_type='%s', prod_brand='%s', prod_color='%s', prod_thicknesses='%s'," \
                        "prod_level='%s', prod_process='%s', prod_unit='%s', prod_remark='%s', prod_specifications='%s', prod_warehouse='%s', prod_price_type='%s', prod_lowest_price='%f'," \
                        "prod_higest_price='%f', prod_average_price='%f', prod_change_amount='%f', prod_change_rate='%f', update_time='%s', plat_source_id='%d'," \
                        "plat_source_name='%s', plat_source_url='%s' where hashKey='%s'"

            updateData = (
                str(dt),
                int(dt_type),
                str(prod_quote_type_name) if prod_quote_type_name else '',
                str(prod_name) if prod_name else '',
                str(prod_factory) if prod_factory else '',
                str(prod_sales_area) if prod_sales_area else '',
                str(prod_area) if prod_area else '',
                str(prod_market) if prod_market else '',
                str(prod_type) if prod_type else '',
                str(prod_brand) if prod_brand else '',
                str(prod_color) if prod_color else '',
                str(prod_thicknesses) if prod_thicknesses else '',
                str(prod_level) if prod_level else '',
                str(prod_process) if prod_process else '',
                str(prod_unit) if prod_unit else '',
                str(prod_remark) if prod_remark else '',
                str(prod_specifications) if prod_specifications else '',
                str(prod_warehouse) if prod_warehouse else '',
                str(prod_price_type) if prod_price_type else '',
                float(prod_lowest_price) if prod_lowest_price else 0.00,
                float(prod_higest_price) if prod_higest_price else 0.00,
                float(prod_average_price) if prod_average_price else 0.00,
                float(prod_change_amount) if prod_change_amount else 0.00,
                float(prod_change_rate) if prod_change_rate else 0.00,
                update_time,
                int(plat_source_id),
                str(plat_source_name),
                str(plat_source_url),
                hashKey
            )

            if insertData and updateData:
                self.UpdateToMysql(conn, insertSql, insertData, updateSql, updateData)
        except Exception as error:
            logger.warning(error)
            logger.warning(data)

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
    def removeStatus(coll, key):
        for num, info in enumerate(coll.find({'$nor': [{'status': None}, {'status': 400}, {'status': 404}]})):
            print(num)
            coll.update_one({key: info[key]}, {'$unset': {'status': ''}}, upsert=True)

    # 多进程获取数据
    def CommandThread(self, proxy=False, history=False, Async=True):
        thread_list = []

        # 设置进程数
        pool = ThreadPool(processes=3)

        """ 每月第一天 """
        if (dtime.today() + datetime.timedelta(days=-dtime.today().day + 1)).replace(hour=0, minute=0, second=0, microsecond=0).day == dtime.today().day:
             # 每月更新总目录
            self.GetCategory()

        # 获取 月数据  共：4334
        categoryData_coll_list = [i for i in self.categoryData_coll.find({"Type": "月均价", 'status': None})]
        for info in categoryData_coll_list:
            if Async:
                out = pool.apply_async(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 异步
            else:
                out = pool.apply(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 同步
            thread_list.append(out)
                # break

        """ 每周第一天 """
        if (pd.to_datetime(str(time.strftime("%Y-%m-%d", time.localtime(time.time())))) - pd.to_datetime('20160103')).days % 7 == 1:
            # 每周更新所有分类下详细产品数据
            for _times in range(3):
                category_coll_list = [i for i in self.category_coll.find(
                    {'$nor': [{'status': 1}, {'status': 404}, {'status': 404}]})]  # category_coll总数: 1017
                for info in category_coll_list:
                    if Async:
                        out = pool.apply_async(func=self.GetCategoryData, args=(info, proxy,))  # 异步
                    else:
                        out = pool.apply(func=self.GetCategoryData, args=(info, proxy,))  # 同步
                    thread_list.append(out)
            #         # break

        # 获取 周数据  共：4334
        categoryData_coll_list = [i for i in self.categoryData_coll.find({"Type": "周均价", 'status': None})]
        for info in categoryData_coll_list:
            if Async:
                out = pool.apply_async(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 异步
            else:
                out = pool.apply(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 同步
            thread_list.append(out)
            # break

        """ 每一天 """
        # 获取 天数据  共：4334
        categoryData_coll_list = [i for i in self.categoryData_coll.find({"Type": "日报价", 'status': None})]
        for info in categoryData_coll_list:
            if Async:
                out = pool.apply_async(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 异步
            else:
                out = pool.apply(func=self.DownloadHistoryData, args=(info, proxy, history,))  # 同步
            thread_list.append(out)
            # break

        pool.close()
        pool.join()


def jlcrun():
    jlc = JinLianChuang()

    # if str(time.strftime("%H", time.localtime(time.time()))) == '10':
    # 清除标记
    jlc.removeStatus(jlc.category_coll, 'link')
    jlc.removeStatus(jlc.categoryData_coll, 'hashKey')

    # 多进程获取数据  params: proxy  history
    jlc.CommandThread(proxy=False, history=True)

    logger.info('jlc 获取历史数据--完成')


if __name__ == '__main__':
    jlcrun()
