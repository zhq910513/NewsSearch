import configparser
import logging
import os
import pprint
import time
from os import path

import requests
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

requests.packages.urllib3.disable_warnings()
pp=pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)

logPath = os.path.join(dh + r'/Logs/OPEC.log')
settingPath = os.path.join(dh + r'/Settings.ini')

if not os.path.isfile(logPath):
    open(logPath,'w')

logger = logging.getLogger(logPath)
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# 读ini文件
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")


class OPEC:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        cookiedb = conf.get("Mongo", "COOKIE")
        cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']
        self.category_coll = client[datadb]['opec_category']
        self.userAgent = UserAgent().random
        self.categoryUrl = 'https://www.opec.org/opec_web/en/publications/5844.htm'
        self.categoryHeaders = {
            'authority': 'www.opec.org',
            'method': 'GET',
            'path': '/opec_web/en/publications/5844.htm',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'WT_FPC=id=b857650a-1391-4f7e-8b19-fbdb4e9db96b:lv=1603683829286:ss=1603683764531',
            'pragma': 'no-cache',
            'referer': 'https://www.opec.org/opec_web/en/publications/338.htm',
            'upgrade-insecure-requests': '1',
            'user-agent': self.userAgent
        }
        self.downloadExcelHeaders = {
            'authority': 'www.opec.org',
            'method': 'GET',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'WT_FPC=id=b857650a-1391-4f7e-8b19-fbdb4e9db96b:lv=1603685692828:ss=1603683764531',
            'pragma': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

    # 获取分类的链接
    def GetCategory(self):
        try:
            resp = requests.get(self.categoryUrl, headers=self.categoryHeaders, verify=False)
            if resp.status_code==200:
                respData = self.ParseCategoryData(resp.text)
                if respData:
                    for item in respData:
                        print(item['month'])
                        try:
                            self.category_coll.update_one({'month': item['month']}, {'$set': item}, upsert=True)
                        except DuplicateKeyError:
                            pass
                        except Exception as error:
                            logger.warning(error)
            else:logger.warning(resp.status_code)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetCategory()
        except Exception as error:
            logger.warning(error)
            return None

    @staticmethod
    def ParseCategoryData(Html):
        soup = BeautifulSoup(Html, 'lxml')
        dataList = []

        try:
            info = soup.find('div', {'class': 'slim'}).find_all('p')[1:]
            numList = [num for num, p in enumerate(info) if '2020' in p.find('strong').get_text()]

            start_end_list = []
            for n in range(len(numList)):
                if n+1 == len(numList):
                    start_end_list.append((numList[n], 0))
                else:
                    start_end_list.append((numList[n], numList[n+1]))

            for index in start_end_list:
                data = {}
                if index[1] != 0:
                    for p in info[index[0]: index[1]]:
                        try:
                            if '2020' in p.get_text().strip():
                                month = p.get_text().strip()
                                data.update({
                                    'month': month
                                })
                        except:
                            pass

                        for a in p.find_all('a'):
                            try:
                                if 'PDF' in a.get_text().strip():
                                    pdfLink = 'https://www.opec.org' + a.get('href')
                                    data.update({
                                        'pdfLink': pdfLink
                                    })
                                elif 'Excel' in a.get_text().strip():
                                    excelLink = 'https://www.opec.org' + a.get('href')
                                    data.update({
                                        'excelLink': excelLink
                                    })
                            except:
                                pass
                    if data:
                        dataList.append(data)
                else:
                    for p in info[index[0]: ]:
                        try:
                            if '2020' in p.get_text().strip():
                                month = p.get_text().strip()
                                data.update({
                                    'month': month
                                })
                        except:
                            pass

                        for a in p.find_all('a'):
                            try:
                                if 'PDF' in a.get_text().strip():
                                    pdfLink = 'https://www.opec.org' + a.get('href')
                                    data.update({
                                        'pdfLink': pdfLink
                                    })
                                elif 'Excel' in a.get_text().strip():
                                    excelLink = 'https://www.opec.org' + a.get('href')
                                    data.update({
                                        'excelLink': excelLink
                                    })
                            except:
                                pass
                    if data:
                        dataList.append(data)
        except Exception as error:
            logger.warning(error)

        if dataList:
            return dataList

    def DownLoad(self):
        for info in self.category_coll.find({'status': None}):
            self.DownLoadExcel(info)
            self.DownLoadPDF(info)
            # break

    def DownLoadExcel(self, info, retry=1):
        filePath = self.downloadPath + '/opec/'
        if not os.path.exists(filePath):
            os.makedirs(filePath)

        # excel
        try:
            excelLink = info.get('excelLink')
            resp = requests.get(excelLink, headers=self.downloadExcelHeaders, verify=False)
            # 存储到本地
            if resp.content:
                fp = filePath + '{}.xlsx'.format(info['month'].replace(' ', '_'))
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()
        except requests.exceptions.ConnectionError:
            if retry<3:
                print('网络问题，重试中...')
                return self.DownLoadExcel(info, retry+1)
        except Exception as error:
            logger.warning(error)

    def DownLoadPDF(self, info, retry=1):
        filePath = self.downloadPath + '/opec/'
        if not os.path.exists(filePath):
            os.makedirs(filePath)

        # pdf
        try:
            pdfLink = info.get('pdfLink')
            resp = requests.get(pdfLink, headers=self.downloadExcelHeaders, verify=False)
            # 存储到本地
            if resp.content:
                fp = filePath + '{}.pdf'.format(info['month'].replace(' ', '_'))
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()
        except requests.exceptions.ConnectionError:
            if retry<3:
                print('网络问题，重试中...')
                return self.DownLoadPDF(info, retry+1)
        except Exception as error:
            logger.warning(error)


class BKXS:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        cookiedb = conf.get("Mongo", "COOKIE")
        cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']
        self.data_coll = client[datadb]['bkxs_data']
        self.userAgent = UserAgent().random
        self.Urls = [
            'https://rigcount.bakerhughes.com/',
            # 'https://rigcount.bakerhughes.com/na-rig-count',
            # 'https://rigcount.bakerhughes.com/intl-rig-count'
        ]
        self.downloadHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'rigcount.bakerhughes.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0'
        }

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

    def GetData(self):
        for link in self.Urls:
            try:
                resp = requests.get(link, self.downloadHeaders, verify=False)
                resp.encoding = 'utf-8'
                if resp.status_code == 200:
                    data = self.ParseHtml(link, resp.text)
                    if data:
                        self.data_coll.update_one({'link': link},
                                                      {'$set': {
                                                          'link': link,
                                                          'data': data
                                                      }}, upsert=True)
            except Exception as error:
                logger.warning(error)

    def ParseHtml(self, link, html):
        soup = BeautifulSoup(html, 'lxml')
        dataList = []
        if str(link).endswith('.com/'):
            titles = [th.get_text().strip() for th in soup.find('table', {'class': 'nirtable collapse-table summary-table'}).find_all('tr')[0].find_all('th')]
            for tr in soup.find('table', {'class': 'nirtable collapse-table summary-table'}).find_all('tr')[1:]:
                data = {}
                if len(tr.find_all('td')) == len(titles):
                    for num in range(len(titles)):
                        data.update({
                            titles[num]: tr.find_all('td')[num].get_text().strip().replace('\n', '').replace('\t', '').replace('\r', '')
                        })
                if data:
                    dataList.append(data)
            return dataList
        elif str(link).endswith('na-rig-count'):
            downloadUrl = soup.find('table', {'class': 'nirtable collapse-table'}).find('tbody').find_all('tr')[0].find('a').get('href')

            resp = requests.get(downloadUrl, headers=self.downloadHeaders, verify=False)
            # 存储到本地
            if resp.content:
                fp = self.downloadPath + '/bkxs/bkxs_na.xlsb'
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()
            return
        elif str(link).endswith('intl-rig-count'):
            downloadUrl = soup.find('table', {'class': 'nirtable collapse-table'}).find('tbody').find_all('tr')[1].find('a').get('href')

            resp = requests.get(downloadUrl, headers=self.downloadHeaders, verify=False)
            # 存储到本地
            if resp.content:
                fp = self.downloadPath + '/bkxs/bkxs_intl.xlsx'
                f = open(fp, "wb")
                f.write(resp.content)
                f.close()
            return
        else:return


class MeiGuoNengYuan:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "QUOTATIONDB")
        client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        # client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        cookiedb = conf.get("Mongo", "COOKIE")
        cookieclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=cookiedb))
        # cookieclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=cookiedb))
        self.cookie_coll = cookieclient[cookiedb]['cookies']
        self.category_coll = client[datadb]['meiguo_category']
        self.userAgent = UserAgent().random
        self.categoryUrl = 'https://www.eia.gov/dnav/pet/PET_SUM_SNDW_A_EPC0_FPF_MBBLPD_W.htm'
        self.categoryHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': 'ak_bmsc=CC5A4DD5863FBCA3787CB0AF4FB3A9C6B833930AE71600003C7D965F56893A6A~plrPC3F7fwxM9RkZugIpUJlSFbN82Q0FLPb2KikbVV7gnP9YPE87avsouufDFgNULP7EzDXLUHgc/a5dFsaulsT7ZaDyxNrCIugCnXDy0eltfk3Z0DyStBN7pRGtbVF0XtIQ1Yt4JK9d0CYdE6Vjxmo6oJkQHbpbQuw9P5QBDy0SC6VDYQqr1J3whDUbVht4k5D41wGjYY+XiKLsOG1r6Dh/Iy2bq/q4aMMn7M7AZ6v5o=; __utma=165580587.702375043.1603697984.1603697984.1603697984.1; __utmc=165580587; __utmz=165580587.1603697984.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; __utmb=165580587.17.8.1603700107321; bm_sv=300343B6A87175486D681729F569A921~wweYovZHL7kTwRUsXLvD6BQW6sGGznwCsO4fAztmRujCx+z/ypdEb38Nk6Jl8cxhQfcp3x5H9W9qor7GP5M7k50u36vriQ4c/5fq0r2o7eyeoxMhIyUbkUSQatOtwrBR6WuQ5pfxdw+RXrNUR+SdLQ==',
            'Host': 'www.eia.gov',
            'Pragma': 'no-cache',
            'Referer': 'https://www.eia.gov/dnav/pet/pet_sum_sndw_a_EPOBGRR_YIR_mbblpd_w.htm',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': self.userAgent
        }
        self.downloadExcelHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': 'ak_bmsc=CC5A4DD5863FBCA3787CB0AF4FB3A9C6B833930AE71600003C7D965F56893A6A~plrPC3F7fwxM9RkZugIpUJlSFbN82Q0FLPb2KikbVV7gnP9YPE87avsouufDFgNULP7EzDXLUHgc/a5dFsaulsT7ZaDyxNrCIugCnXDy0eltfk3Z0DyStBN7pRGtbVF0XtIQ1Yt4JK9d0CYdE6Vjxmo6oJkQHbpbQuw9P5QBDy0SC6VDYQqr1J3whDUbVht4k5D41wGjYY+XiKLsOG1r6Dh/Iy2bq/q4aMMn7M7AZ6v5o=; __utma=165580587.702375043.1603697984.1603697984.1603697984.1; __utmc=165580587; __utmz=165580587.1603697984.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); bm_sv=300343B6A87175486D681729F569A921~wweYovZHL7kTwRUsXLvD6BQW6sGGznwCsO4fAztmRujCx+z/ypdEb38Nk6Jl8cxhQfcp3x5H9W9qor7GP5M7k50u36vriQ4c/5fq0r2o7eyOqnp+s9g5KikcxKLVkFPPm+XBf+2ZvjCy/koDqfLORA==; __utmb=165580587.28.8.1603703462440',
            'Host': 'www.eia.gov',
            'Pragma': 'no-cache',
            'Referer': 'https://www.eia.gov/dnav/pet/pet_sum_sndw_a_(na)_YRL_mbblpd_w.htm',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': self.userAgent
        }

        # 下载文件存放地址
        self.downloadPath = conf.get("DownloadPath", "PATH")

    # 获取分类的链接
    def GetCategory(self):
        try:
            resp = requests.get(self.categoryUrl, headers=self.categoryHeaders, verify=False)
            if resp.status_code==200:
                respData = self.ParseCategoryData(resp.text)
                if respData:
                    for item in respData:
                        print(item['name'])
                        try:
                            self.category_coll.insert_one(item)
                        except DuplicateKeyError:
                            pass
                        except Exception as error:
                            logger.warning(error)
            else:logger.warning(resp.status_code)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetCategory()
        except Exception as error:
            logger.warning(error)
            return None

    @staticmethod
    def ParseCategoryData(Html):
        soup = BeautifulSoup(Html, 'lxml')
        dataList = []

        try:
            for option in soup.find('select', {'name': 'Drop1'}).find_all('option'):
                try:
                    link = 'https://www.eia.gov/dnav/pet/xls/' + option.get('value').split('.')[0].upper() + '.xls'
                except:
                    link = None

                try:
                    name = option.get_text().replace('\t', '').replace('\r', '').replace('&', '').replace('<', '').replace('>', '').strip()
                except:
                    name = None

                if link and name:
                    dataList.append({
                        'name': name,
                        'link': link
                    })
                else:
                    pass
            if dataList:
                return dataList
            else:
                return None
        except Exception as error:
            logger.warning(error)

    def mongodb_to_excel(self, query, num=0):
        filePath = self.downloadPath + '/meiguo/'
        if not os.path.exists(filePath):
            os.makedirs(filePath)

        for num, item in enumerate(self.category_coll.find(query),start=num):
            url = item.get('link')

            fileName = item.get('name').replace('.', '').replace(',', '').replace('-', '').replace(' ', '_').replace('/', '').replace('__', '_')
            fp = filePath + '{}.xls'.format(fileName)
            print(fp)

            try:
                resp = requests.get(url, headers=self.downloadExcelHeaders, verify=False)

                if resp.content:
                    f = open(fp, "wb")
                    f.write(resp.content)
                    f.close()

                    self.category_coll.update_one({"link": item['link']}, {'$set': {'status': 1}}, upsert=True)

                time.sleep(3)
            except requests.exceptions.ConnectionError:
                print('网络问题，重试中...')
                return self.mongodb_to_excel(query, num)
            except Exception as error:
                logger.warning(error)

            # break


def run():
    # opec = OPEC()
    # opec.GetCategory()
    # opec.DownLoad()

    # bkxs = BKXS()
    # bkxs.GetData()

    mg = MeiGuoNengYuan()
    # mg.GetCategory()
    mg.mongodb_to_excel({'status': None})


if __name__ == '__main__':
    run()
