import configparser
import logging
import os
import pprint
import time
from os import path

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

requests.packages.urllib3.disable_warnings()
pp=pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)

logPath = os.path.join(dh + r'/Logs/meiguonengyuan.log')
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
        for num, item in enumerate(self.category_coll.find(query),start=num):
            print(item.get('name'))
            fileName = item.get('link').split('/')[-1]
            url = item.get('link')

            filePath = r'F:/zhuochuang/{}'.format(fileName)
            print(filePath)

            if not os.path.exists(r'F:/zhuochuang'):
                os.makedirs(r'F:/zhuochuang')

            try:
                resp = requests.get(url, headers=self.downloadExcelHeaders, verify=False)

                if resp.content:
                    f = open(filePath, "wb")
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


if __name__ == '__main__':
    mg =  MeiGuoNengYuan()

    # mg.GetCategory()

    mg.mongodb_to_excel({})

