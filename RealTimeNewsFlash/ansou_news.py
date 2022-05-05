#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import sys

sys.path.append("../")
import configparser
import hashlib
import json
import logging
import os
import pprint
import re
import shutil
import time
from multiprocessing.pool import ThreadPool
from os import path

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from dingtalkchatbot.chatbot import DingtalkChatbot

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)

settingPath = os.path.join(dh + r'/Settings.ini')
logPath = os.path.join(dh + r'/Logs/ansou.log')
imagePath = os.path.join(dh + r'/ImageVideoData/ansou_image')
videoPath = os.path.join(dh + r'/ImageVideoData/ansou_videos')

if not os.path.isfile(logPath):
    open(logPath, 'w+')
if not os.path.exists(imagePath):
    os.makedirs(imagePath)
if not os.path.exists(videoPath):
    os.makedirs(videoPath)

logger = logging.getLogger()
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# 读ini文件
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")

class TopicPageNews:
    def __init__(self):
        # 图片储存服务器
        self.upload_server = conf.get("UploadServer", "SERVER")

        # 实例化 Mongo
        datadb = conf.get("Mongo", "NEWSFLASHDB")
        proxydb = conf.get("Mongo", "PROXY")

        # client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))

        # proxyclient = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=proxydb))
        proxyclient = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=proxydb))
        self.proxy_coll = proxyclient[proxydb]['proxies']
        self.pros = [pro.get('pro') for pro in self.proxy_coll.find({'status': 1})]

        self.id_coll = client[datadb]['ansou_topic']
        self.userAgent = UserAgent().random
        self.postHomepageHeaders = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.antsoo.com',
            'User-Agent': self.userAgent
        }
        self.getHeaders = {
            'authority': 'www.antsoo.com',
            'method': 'GET',
            'path': '/topic/topicDetails?id=14547&from=report',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            # 'cookie': 'acw_tc=74fd1d1915971370031637884eb4c9b6ce945ed20bd5448f13756f8715',
            'pragma': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': self.userAgent
        }
        self.picHeaders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': '139oss.oss-cn-shanghai.aliyuncs.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.albumListHeaders = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.antsoo.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.articleHeaders = {
            'accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '532',
            'Content-Type': 'application/json',
            'Host': '8.129.215.170:8855',
            'Origin': 'http://8.129.215.170:8855',
            'Pragma': 'no-cache',
            # 'Referer': 'http://8.129.215.170:8855/swagger-ui.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.moreInfoHeaders = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.antsoo.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.postHomepageUrl = 'https://www.antsoo.com/api/appTopic/topicList'
        self.getUrl = 'https://www.antsoo.com/topic/topicDetails?id={}&from=report'
        self.albumNewsUrl = 'https://www.antsoo.com/topic/topicDetails?id={0}&messageAlbumId={1}&from=messageAlbumList'
        self.albumListUrl = 'https://wwwapi.antsoo.com/appTopic/appSubjectList'
        self.albumNewsListUrl = 'https://wwwapi.antsoo.com/appTopic/appTopicList'

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

    # 请求 首页内容
    def PostHomepageData(self, proxy=False, history=False):
        """
        :return:
        """
        if history:
            num = 51
        else:
            num = 2

        for page in range(1, num):
            print('*******   翻第 {} 页   *******'.format(page))
            jsonData = {
                'pIndex': page,
                'pSize': 100,
                'timeType': 0,
                'topicLabelType': 0
            }
            try:
                if proxy:
                    pro = self.GetProxy()
                    if pro:
                        resp = requests.post(self.postHomepageUrl, headers=self.postHomepageHeaders, proxies=pro,
                                             json=jsonData, verify=False, timeout=10)
                    else:
                        resp = requests.post(self.postHomepageUrl, headers=self.postHomepageHeaders, json=jsonData,
                                             verify=False, timeout=10)
                else:
                    resp = requests.post(self.postHomepageUrl, headers=self.postHomepageHeaders, json=jsonData,
                                         verify=False, timeout=10)
                if resp.status_code == 200:
                    self.ParseHomepageNews(resp.json(), None, proxy)
                else:
                    return None
            except requests.exceptions.ConnectionError:
                print('ConnectionError')
                return self.PostHomepageData()
            except TimeoutError:
                logger.warning('翻第 {} 页 TimeoutError'.format(page))
            except Exception as error:
                logger.warning(error)

    # 从 首页或者专辑 中获取 全部文章
    def ParseHomepageNews(self, htmlJson: dict, dataType, proxy=False):
        """
        :param htmlJson:
        :param dataType:
        :return:
        """
        news_list = []
        if isinstance(htmlJson, dict):
            if isinstance(htmlJson.get('body').get('topicList'), list):
                topicList = htmlJson.get('body').get('topicList')
            elif isinstance(htmlJson.get('body').get('appTopicList'), list):
                topicList = htmlJson.get('body').get('appTopicList')
            else:
                topicList = None

            if topicList:
                for topic in topicList:
                    news = {}
                    # albumId
                    try:
                        if dataType:
                            news.update({'albumId': dataType})
                        else:
                            pass
                    except:
                        pass

                    # createTime
                    try:
                        createTime = topic.get('createTime')
                        if '小时' in createTime:
                            t = int(createTime.split('小时')[0])
                            timeStamp = time.time() - t * 60 * 60
                            timeArray = time.localtime(timeStamp)
                            createTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                            if createTime:
                                news.update({'createTime': createTime})
                    except:
                        pass

                    # news-id
                    try:
                        news_id = topic.get('id')
                        if news_id:
                            news.update({'news_id': news_id})
                    except:
                        pass

                    # title-picture
                    try:
                        if topic.get('titleImage'):
                            title_picture = topic.get('titleImage')
                        elif topic.get('imageUrl'):
                            title_picture = topic.get('imageUrl')
                        else:
                            title_picture = None
                        if title_picture:
                            news.update({'title_picture': title_picture})
                    except:
                        pass

                    # title-text
                    try:
                        title_text = topic.get('title')
                        if title_text:
                            news.update({'title_text': title_text})
                    except:
                        pass

                    # title-desc
                    try:
                        title_desc = topic.get('summary')
                        if title_desc:
                            news.update({'title_desc': self.illegal_char(title_desc)})
                    except:
                        pass

                    # view-count
                    try:
                        view_count = topic.get('viewCount')
                        if view_count:
                            news.update({'view_count': view_count})
                    except:
                        pass

                    if news:
                        # print(news)
                        news_list.append(news)
        if news_list:
            # print(news_list)
            self.HomepageNewsList(news_list, proxy)
        else:
            return None

    # 逐条 访问 首页/专辑 中全部文章
    def HomepageNewsList(self, respData: list, proxy=False):
        """
        :param respData:
        """
        if respData:
            for news in respData:
                try:
                    self.id_coll.insert_one({'news_id': news['news_id']})
                    print('即将获取文章  {}！'.format(news['news_id']))
                    self.GetNewsData(news, proxy)
                except DuplicateKeyError:
                    print('文章id  {}  已存在数据库！'.format(news['news_id']))
                    pass
                except Exception as error:
                    logger.warning(error)

    # 请求 具体 文章内容
    def GetNewsData(self, newsInfo: dict, proxy=False):
        """
        :param newsInfo:
        :return:
        """
        if newsInfo.get('albumId'):
            headers = self.getHeaders.update({
                'path': '/topic/topicDetails?id={0}&messageAlbumId={1}&from=messageAlbumList'.format(
                    newsInfo['news_id'], newsInfo['albumId'])})
            baseUrl = self.albumNewsUrl.format(newsInfo['news_id'], newsInfo['albumId'])
        else:
            headers = self.getHeaders.update(
                {'path': '/topic/topicDetails?id={}&from=report'.format(newsInfo['news_id'])})
            baseUrl = self.getUrl.format(newsInfo['news_id'])
        try:
            if proxy:
                pro = self.GetProxy()
                if pro:
                    resp = requests.get(baseUrl, headers=headers, proxies=pro, timeout=10)
                else:
                    resp = requests.get(baseUrl, headers=headers, timeout=10)
            else:
                resp = requests.get(baseUrl, headers=headers, timeout=10)
            # print(resp.status_code)
            if resp.status_code == 200:
                resp.encoding = 'utf-8'
                newsInfo.update({'sourceLink': baseUrl})
                return self.ParseNewsInfo(resp.text, newsInfo)
        except requests.exceptions.ConnectionError:
            return self.GetNewsData(newsInfo)
        except Exception as error:
            logger.warning(error)

    # 解析 具体 文章数据
    def ParseNewsInfo(self, html: str, info):
        """
        :param html:
        :param info:
        """
        soup = BeautifulSoup(html, 'lxml')
        data = {}
        imageList = []
        if soup.find('div', {'class': 'detailContent'}):
            # css
            try:
                data.update({'css_html': str(soup.find('div', {'class': 'detailContent'}))})
            except:
                pass

            # view-count
            try:
                view_count = soup.find('div', {'class': 'time clearfix'}).find('h4').get_text().strip().replace('\n',
                                                                                                                '').replace(
                    '\t', '').replace('\r', '')
                if view_count:
                    data.update({'view_count': int(view_count)})
            except:
                pass

            # descContent
            try:
                article = []

                for p in soup.find('div', {'class': 'descContent'}).find_all('p'):
                    # text
                    try:
                        content = p.get_text()
                        if content:
                            content = re.sub('\s', ' ', content).strip().replace('\n', '').replace('\t', '').replace(
                                '\r', '')
                            if content and content not in article:
                                if '文章来源：' in content:
                                    data.update({'文章来源': content.split('：')[1]})
                                else:
                                    article.append(content)
                            else:
                                pass
                    except:
                        pass

                    # image
                    try:
                        image = p.find('img').get('src')
                        if image and image not in article:
                            if image == info.get('title_picture'):
                                pass
                            else:
                                imageList.append(image)
                                article.append(image)
                    except:
                        pass

                if article:
                    data.update({'article': article})
            except:
                pass

            if data:
                data.update(info)
                # pp.pprint(data)

        if info.get('title_picture'):
            # data_type : 2 素材标题图, 3 相关图片，4 正文图片
            self.DownloadImageThread([info['title_picture']], pic_info={'pic_type': 2, 'id': info['news_id']},
                                     num_processes=4, remove_bad=True)

        if imageList:
            # data_type : 2 素材标题图, 3 相关图片，4 正文图片
            self.DownloadImageThread(imageList, pic_info={'pic_type': 4, 'id': info['news_id']}, num_processes=4,
                                     remove_bad=True)

        # 下载/上传图片
        try:
            # 图片数量
            print('{0} 文章中有 {1} 张图片'.format(info['news_id'], len(os.listdir(imagePath))))

            # 清除图片文件夹
            shutil.rmtree(imagePath, True)
            os.mkdir(imagePath)
        except Exception as error:
            logger.warning('删除本地图片文件/文件夹出错！！！{}'.format(error))

        # 上传文章
        try:
            self.UploadArticle(imageList, info, data)
        except Exception as error:
            logger.warning('上传文章出错！！！{}'.format(error))

    # 请求 所有 专辑模块
    def AlbumListPost(self, pageStart=1, proxy=False):
        """
        :param pageStart:
        :return:
        """
        for page in range(pageStart, 51):
            jsonData = {
                'pIndex': page,
                'pSize': 100
            }
            try:
                if proxy:
                    pro = self.GetProxy()
                    if pro:
                        resp = requests.post(self.albumListUrl, headers=self.albumListHeaders, proxies=pro,
                                             json=jsonData, timeout=10)
                    else:
                        resp = requests.post(self.albumListUrl, headers=self.albumListHeaders, json=jsonData,
                                             timeout=10)
                else:
                    resp = requests.post(self.albumListUrl, headers=self.albumListHeaders, json=jsonData, timeout=60)

                if resp.status_code == 200:
                    dataJson = resp.json()
                    self.AlbumNewsListPost(dataJson, 0, 1, proxy)
                    if len(dataJson.get('body').get('subjectList')) < 80:
                        break
                    else:
                        pass
                else:
                    self.AlbumListPost(page + 1)
            except requests.exceptions.ConnectionError:
                return self.AlbumListPost(page, proxy)
            except Exception as error:
                logger.warning(error)
                return None

    # 请求 专辑模块 下的资讯内容
    def AlbumNewsListPost(self, htmlJson: dict, break_index=0, pageNum=1, proxy=False):
        """
        :param htmlJson:
        :param break_index:
        :param pageNum:
        :return:
        """
        if htmlJson.get('body').get('subjectList') and isinstance(htmlJson.get('body').get('subjectList'), list):
            for sub_index, subject in enumerate(htmlJson.get('body').get('subjectList')[break_index:]):
                print('****************** 第 {0} 个模块 {1} ******************'.format(sub_index, subject))
                for page in range(pageNum, 51):
                    jsonData = {
                        'pIndex': page,
                        'pSize': 100,
                        'subjectId': subject['id'],
                        'userId': 'null'
                    }
                    try:
                        if proxy:
                            pro = self.GetProxy()
                            if pro:
                                resp = requests.post(self.albumNewsListUrl, headers=self.albumListHeaders, proxies=pro,
                                                     json=jsonData, timeout=10)
                            else:
                                resp = requests.post(self.albumNewsListUrl, headers=self.albumListHeaders,
                                                     json=jsonData, timeout=10)
                        else:
                            resp = requests.post(self.albumNewsListUrl, headers=self.albumListHeaders, json=jsonData,
                                                 timeout=10)

                        if resp.status_code == 200:
                            dataJson = resp.json()
                            self.ParseHomepageNews(dataJson, subject['id'])
                            if len(dataJson.get('body').get('appTopicList')) <= 1:
                                break
                            else:
                                pass
                        else:
                            pass
                    except requests.exceptions.ConnectionError:
                        return self.AlbumNewsListPost(htmlJson, sub_index, page, proxy)
                    except Exception as error:
                        logger.warning(error)
                        return None

    # 构建多线程 下载 图片
    def DownloadImageThread(self, url_list, pic_info, num_processes, remove_bad=False, Async=True):
        """
        多线程下载图片
        :param pic_info:
        :param url_list: image url list
        :param num_processes: 开启线程个数
        :param remove_bad: 是否去除下载失败的数据
        :param Async:是否异步
        :return: 返回图片的存储地址列表
        """
        # 开启多线程
        pool = ThreadPool(processes=num_processes)
        thread_list = []
        for image_url in url_list:
            if Async:
                out = pool.apply_async(func=self.DownloadPicture, args=(image_url, pic_info))  # 异步
            else:
                out = pool.apply(func=self.DownloadPicture, args=(image_url,))  # 同步
            thread_list.append(out)

        pool.close()
        pool.join()

        # 获取输出结果
        image_list = []
        if Async:
            for p in thread_list:
                image = p.get()  # get会阻塞
                image_list.append(image)
        else:
            image_list = thread_list
        if remove_bad:
            image_list = [i for i in image_list if i is not None]
        return image_list

    # 下载/上传 图片 函数
    def DownloadPicture(self, url, pic_info, retry=1):
        """
        根据url下载图片
        :param pic_info:
        :param url:
        :return: 返回保存的图片途径
        """
        try:
            res = requests.get(url, timeout=20)

            if res.status_code == 200:
                basename = hashlib.md5(url.encode("utf8")).hexdigest() + '.jpg'
                filename = os.path.join(imagePath + '/' + basename)
                with open(filename, "wb") as f:
                    content = res.content
                    f.write(content)
                    f.close()
                print('图片下载成功 -- {}'.format(url))

                # upload picture
                uploadUrl = 'http://{0}/api/common/upload?composeId={0}&type={1}&isNameReal=0'.format(self.upload_server, pic_info['id'], pic_info['pic_type'])

                files = {
                    'file': (basename, open(filename, 'rb'), 'image/jpeg')
                }

                try:
                    resp = requests.post(url=uploadUrl, files=files, timeout=20)
                    if resp.json().get('message') == '携带数据成功':
                        print("文章id {0} *** type {1} *** download image successfully:{2} *** upload status {3}".format(
                            pic_info['id'], pic_info['pic_type'], url, resp.json().get('code')))
                    else:
                        logger.warning(resp.json())
                except requests.exceptions.ConnectionError:
                    if retry < 3:
                        return self.DownloadPicture(url, pic_info, retry+1)
                    else:
                        self.SendMeaasge(self.upload_server)
                except Exception as error:
                    logger.warning(error)
        except requests.exceptions.ConnectionError:
            return self.DownloadPicture(url, pic_info)
        except Exception as error:
            logger.warning(error)
            return None
        return None

    # 清洗 文本 ASCII
    @staticmethod
    def illegal_char(s):
        """

        :param s:
        :return:
        """
        s = re.compile(u"[^"
                       u""u"\u4e00-\u9fa5"
                       u""u"\u0041-\u005A"
                       u"\u0061-\u007A"
                       u"\u0030-\u0039"
                       u"\u3002\uFF1F\uFF01\uFF0C\u3001\uFF1B\uFF1A\u300C\u300D\u300E\u300F\u2018\u2019\u201C\u201D\uFF08\uFF09\u3014\u3015\u3010\u3011\u2014\u2026\u2013\uFF0E\u300A\u300B\u3008\u3009"
                       u"\!\@\#\$\%\^\&\*\(\)\-\=\[\]\{\}\\\|\;\'\:\"\,\.\/\<\>\?\/\*\+"
                       u"]+").sub('', s)
        return s

    # 上传 文章
    def UploadArticle(self, imageList, info, data, retry=1):
        """

        :param imageList:
        :param info:
        :param data:
        :return:
        """
        # 上传文章
        try:
            serverUrl = 'https://zuiyouliao-prod.oss-cn-beijing.aliyuncs.com/zx/image/'
            newImageList = []
            cssHtml = data.get('css_html')
            if imageList:
                if info.get('title_picture'):
                    imageList.append(info['title_picture'])

                for imageOld in imageList:
                    basename = hashlib.md5(imageOld.encode("utf8")).hexdigest() + '.jpg'
                    imageNew = serverUrl + basename
                    newImageList.append(imageNew)

                    try:
                        if imageOld == data['title_picture']:
                            data['title_picture'] = imageNew
                    except:
                        pass

                    articleContent = data['article']
                    for pic_index, article in enumerate(articleContent):
                        if article == imageOld:
                            articleContent[pic_index] = imageNew

                    data['article'] = articleContent

                    cssHtml = cssHtml.replace(imageOld, imageNew)

            # upload
            articleUpload = 'http://{0}/articleMaterials/add'.format(self.upload_server)
            dataJson = {
                'content': ' '.join(data.get('article')) if isinstance(data.get('article'), list) else str(
                    data.get('article')),  # string正文
                'contentImageUrls': newImageList,  # [...]
                'createTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # string($date-time)创建时间
                'id': str(data.get('news_id')),  # string素材文章id
                'readCount': int(data.get('view_count')),  # integer($int32)阅读量
                'releaseTime': data.get('createTime'),  # string($date-time)发布时间
                'source': data.get('文章来源') if data.get('文章来源') else '',  # string来源网站
                'sourceLink': data.get('sourceLink'),  # string来源链接
                'titleImageUrls': [data.get('title_picture')],  # [...]
                'titleName': data.get('title_text'),  # string素材标题
                'type': 2,  # integer($int32)2 资讯 3快讯
                'updateTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # string($date-time)更新时间
                'cssHtml': cssHtml
            }
            try:
                resp = requests.post(url=articleUpload, headers=self.articleHeaders, data=json.dumps(dataJson),
                                     timeout=60)
                if resp.json().get('ok'):
                    print("文章id {0} *** upload successfully *** upload status {1}".format(dataJson.get('id'),
                                                                                          resp.json().get('code')))
                elif resp.json().get('status') == 500 and 'DuplicateKey' in resp.json().get('exception'):
                    print('DuplicateKey')
                    pass
                else:
                    logger.warning(resp.json())
            except requests.exceptions.ConnectionError:
                if retry < 3:
                    return self.UploadArticle(imageList, info, data, retry+1)
                else:
                    self.SendMeaasge(self.upload_server)
            except Exception as error:
                logger.warning(error)
        except Exception as error:
            logger.warning(error)

    # 发送警报
    def SendMeaasge(self, server_ip, retry=2):
        webhook = 'https://oapi.dingtalk.com/robot/send?' \
                  'access_token=ae5ee6aad7142340e40194f90b0bfcfed510e568ccfb781e942e18deb7195ea2'

        try:
            message = DingtalkChatbot(webhook)
            message.send_markdown(title="SpiderStatus",
                                  text="Date: {0}\n\n"
                                       "------------------------------------\n\n\n"
                                       " 无法连接服务器{1}，请检查图片/文章存储服务器！！！\n\n\n".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), server_ip))
        except Exception as e:
            logging.warning(e)
            if retry > 0:
                return self.SendMeaasge(retry - 1)
            else:
                logging.warning('The number of retries has been exhaustes !')
                pass


if __name__ == '__main__':
    t = TopicPageNews()
    t.PostHomepageData(proxy=False)
    t.AlbumListPost(proxy=False)
