#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import sys
sys.path.append("../")
import configparser
import datetime
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
from urllib.parse import quote

import requests
from fake_useragent import UserAgent
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from urllib3 import encode_multipart_formdata
from Cookies.proxy import GetProxy

requests.packages.urllib3.disable_warnings()
pp=pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = path.dirname(df)

logPath = os.path.join(dh + r'/Logs/jinshi.log')
settingPath = os.path.join(dh + r'/Settings.ini')
imagePath = os.path.join(dh + r'/ImageVideoData/jinshi_image')
videoPath = os.path.join(dh + r'/ImageVideoData/jinshi_videos')

if not os.path.isfile(logPath):
    open(logPath,'w+')
if not os.path.exists(imagePath):
    os.makedirs(imagePath)
if not os.path.exists(videoPath):
    os.makedirs(videoPath)

logger = logging.getLogger(logPath)
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# 读ini文件
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")

proPath = r'/home/zyl/NewsSearch/Cookies/proxies.txt'
with open(proPath, 'r', encoding='utf-8') as f:
    proxies = f.readlines()


class TopicPageNews:
    def __init__(self):
        # 实例化 Mongo
        datadb = conf.get("Mongo", "NEWSFLASHDB")
        # client = MongoClient('mongodb://127.0.0.1:27017/{db}'.format(db=datadb))
        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/{db}'.format(db=datadb))
        self.id_coll = client[datadb]['jinshi_topic']
        self.userAgent=UserAgent().random
        self.homepageUrl = 'https://www.jin10.com/flash_newest.js'
        self.moreInfoUrl = 'https://flash-api.jin10.com/get_flash_list?max_time={}&channel=-8200'
        self.homepageHeaders = {
            'authority': 'www.jin10.com',
            'method': 'GET',
            'path': '/flash_newest.js',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'referer': 'https://www.jin10.com/',
            'user-agent': self.userAgent
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
        self.videoPageHeaders = {
            'authority': 'v.jin10.com',
            'method': 'GET',
            'path': '/details.html?id=12574',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.videoUploadHeaders = {
            'accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '25',
            'Content-Type': 'application/json',
            'Host': '8.129.215.170:8855',
            'Origin': 'http://8.129.215.170:8855',
            'Pragma': 'no-cache',
            'Referer': 'http://8.129.215.170:8855/swagger-ui.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.picHeaders = {
            'accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': '27.150.182.135:8855',
            'Origin': 'http://8.129.215.170:8855',
            'Pragma': 'no-cache',
            'Referer': 'http://8.129.215.170:8855/swagger-ui.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
        }
        self.moreInfoHeaders = {
            'Accept': '*/*',
            'Origin': 'https://www.jin10.com',
            'Referer': 'https://www.jin10.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
            'x-app-id': 'SO1EJGmNgCtmpcPF',
            'x-version': '1.0.0'
        }
        self.startTime = '2020-08-18 11:50:58'
        self.videoTrueLink = {}
        self.proxies = proxies

    # 请求 首页内容
    def GetHomepageData(self):
        try:
            resp = requests.get(self.homepageUrl, headers=self.homepageHeaders, verify=False, timeout=60)
            if resp.status_code==200:
                respData =resp.text.split('},{"')
                # pp.pprint(respData)
                self.ParseHomepageNews(respData)
            else:logger.warning(resp.status_code)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetHomepageData()
        except Exception as error:
            logger.warning(error)
            return None

    # 查看 更多快讯
    def GetMorePageNews(self, startTime):
        moreInfoUrl = self.moreInfoUrl.format(quote(startTime))
        try:
            resp = requests.get(moreInfoUrl, headers=self.moreInfoHeaders, verify=False, timeout=60)
            if resp.status_code==200:
                self.ParseHomepageNews(resp.json())
            else:logger.warning(resp.status_code)
        except requests.exceptions.ConnectionError:
            print('网络问题，重试中...')
            return self.GetMorePageNews(startTime)
        except Exception as error:
            logger.warning(error)

    # 从 首页 中获取 全部文章
    def ParseHomepageNews(self, htmlJson):
        try:
            news_list = []
            # 查看更多的快讯
            if  isinstance(htmlJson, dict):
                if isinstance(htmlJson.get('data'), list):
                    topicList = htmlJson.get('data')
                else:
                    topicList = None

                if topicList:
                    for news_info in topicList:
                        # createTime
                        try:
                            createTime = news_info.get('time')
                        except:
                            createTime = None
                            pass

                        # news-id
                        try:
                            news_id = news_info.get('id')
                        except:
                            news_id = None
                            pass

                        # title-picture
                        try:
                            title_picture = news_info.get('data').get('pic')
                        except:
                            title_picture = None
                            pass

                        # title-text
                        try:
                            title_text = news_info.get('data').get('content').replace('<b>', '').replace(
                                '</b>', '').replace('<br/>', '').replace('<br />', '').strip()
                        except:
                            title_text = None
                            pass

                        # about-link
                        try:
                            links = []
                            if news_info.get('remark'):
                                for about_info in news_info.get('remark'):
                                    if about_info.get('link'):
                                        links.append(about_info.get('link'))
                                    else:pass
                            else:pass
                            if links:
                                about_link = links
                            else:about_link = None
                        except:
                            about_link = None
                            pass

                        if title_text and '<a href=' in title_text:
                            pass
                        else:
                            news_list.append({
                                'createTime': createTime,
                                'news_id': news_id,
                                'title_picture': title_picture,
                                'title_text': title_text,
                                'about_link': about_link
                            })

            # 首页的快讯
            elif isinstance(htmlJson, list):
                for news_info in htmlJson:
                    if news_info:
                        # createTime
                        try:
                            utc = re.findall('"time":"(.*?)"', news_info, re.S)[0]
                            UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
                            utcTime = datetime.datetime.strptime(utc, UTC_FORMAT)
                            createTime = utcTime + datetime.timedelta(hours=8)
                        except:
                            createTime = None
                            pass

                        # news-id
                        try:
                            news_id = re.findall('id":"(.*?)"', news_info, re.S)[0]
                        except:
                            news_id = None
                            pass

                        # title-picture
                        try:
                            picture = re.findall('"pic":"(.*?)"', news_info, re.S)
                            if picture:
                                title_picture = picture[0]
                            else: title_picture = None
                        except:
                            title_picture = None
                            pass

                        # title-text
                        try:
                            title_text = re.findall('"content":"(.*?)"', news_info, re.S)[0].replace('<b>', '').replace('</b>', '').replace('<br/>', '').replace('<br />', '').strip()
                        except:
                            title_text = None
                            pass

                        # about-link
                        try:
                            about_link = re.findall('"link":"(.*?)"', news_info, re.S)
                        except:
                            about_link = None
                            pass

                        if title_text and '<a href=' in title_text:
                            pass
                        else:
                            news_list.append({
                            # pp.pprint({
                                'createTime': createTime,
                                'news_id': news_id,
                                'title_picture': title_picture,
                                'title_text': title_text,
                                'about_link': about_link
                            })

            if news_list:
                startTime = news_list[-1].get('createTime')
                if startTime:
                    try:
                        self.id_coll.insert_one({'news_id': news_list[-1].get('news_id')})
                        self.startTime = startTime
                    except:
                        self.startTime = None
                self.HomepageNewsList(news_list)
            else:pass
        except Exception as error:
            logger.warning(error)

    # 逐条 访问 首页/专辑 中全部文章
    def HomepageNewsList(self, respData:list):
        if respData:
            for news in respData:
                try:
                    self.id_coll.insert_one({'news_id': news['news_id']})
                    self.ParseNewsInfo(news)
                except DuplicateKeyError:
                    print('文章id  {}  已存在数据库！'.format(news['news_id']))
                    pass
                except Exception as error:
                    logger.warning(error)

            if self.startTime:
                print('开始搜索更多快讯！！！   {}'.format(self.startTime))
                self.GetMorePageNews(str(self.startTime))

    # 解析 具体 文章数据
    def ParseNewsInfo(self, info):
        if info.get('title_picture'):
            # data_type : 2 素材标题图, 3 相关图片，4 正文图片
            self.DownloadImageThread([info['title_picture']], pic_info={'pic_type': 2, 'id': info['news_id']}, num_processes=4, remove_bad=True, Async=True)

        if info.get('about_link'):
            # data_type : 2 素材标题图, 3 相关图片，4 正文图片
            videoTrueLink = self.DownloadImageThread(info.get('about_link'), pic_info={'pic_type': 3, 'id': info['news_id']}, num_processes=4, remove_bad=True, Async=True)
            if videoTrueLink:
                print(videoTrueLink)

        # 下载/上传图片
        try:
            # 图片数量
            if len(os.listdir(imagePath)) > 0:
                print('{0} 文章中有 {1} 张图片'.format(info['news_id'], len(os.listdir(imagePath))))
            # 视频数量
            if len(os.listdir(videoPath)) > 0:
                print('{0} 文章中有 {1} 个视频'.format(info['news_id'], len(os.listdir(videoPath))))

            # 清除图片文件夹
            shutil.rmtree(imagePath, True)
            os.mkdir(imagePath)
            # 清除视频文件夹
            items = os.listdir(videoPath)
            if items:
                for item in items:
                    os.remove(os.path.join(videoPath + '/' + item))
        except Exception as error:
            logger.warning('删除本地图片文件/文件夹出错！！！{}'.format(error))

        # 上传文章
        try:
            self.UploadArticle(info)
        except Exception as error:
            logger.warning('上传文章出错！！！{}'.format(error))

    # 构建多线程 下载 图片
    def DownloadImageThread(self, url_list, pic_info, num_processes, remove_bad=False, Async=True):
        """
        多线程下载图片
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
                out = pool.apply_async(func=self.DownloadPicture_Video, args=(image_url, pic_info, 0, ))  # 异步
            else:
                out = pool.apply(func=self.DownloadPicture_Video, args=(image_url, pic_info, 0, ))  # 同步
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
    def DownloadPicture_Video(self, url, pic_info, retry=0):
        """
        根据url下载
        :param url:
        :return: 返回保存的途径
        """
        # 图片
        if url.endswith('.jpg') or url.endswith('.png') or url.endswith('.pdf') or url.endswith('.gif'):
            try:
                res = requests.get(url, timeout=60)
                if res.status_code == 200:
                    basename = hashlib.md5(url.encode("utf8")).hexdigest() + '.' + url.split('.')[-1]
                    filename = os.path.join(imagePath + '/' + basename)
                    with open(filename, "wb") as f:
                        content = res.content
                        f.write(content)

                    # upload picture
                    uploadUrl = 'http://27.150.182.135:8855/api/common/upload?composeId={0}&type={1}&isNameReal=0'.format(pic_info['id'], pic_info['pic_type'])

                    files = {
                        'file': (basename, open(filename, 'rb'), 'image/jpg')
                    }

                    self.picHeaders.update({
                        'Content-Length': str(os.path.getsize(filename))
                    })

                    try:
                        resp = requests.post(url=uploadUrl, headers=self.picHeaders, files=files, timeout=60)
                        if resp.json().get('message') == '携带数据成功':
                            print("文章id {0} *** type {1} *** download image successfully:{2} *** upload status {3}".format(pic_info['id'], pic_info['pic_type'], url, resp.json().get('code')))
                        else:
                            logger.warning(resp.json())
                    except requests.exceptions.ConnectionError:
                        print('网络问题，重试中...')
                        return self.DownloadPicture_Video(url, pic_info)
                    except Exception as error:
                        logger.warning(error)
                        logger.warning(uploadUrl)
            except requests.exceptions.ConnectionError:
                print('网络问题，重试中...')
                return self.DownloadPicture_Video(url, pic_info)
            except Exception as error:
                logger.warning(error)
                return None
            return None

        # 视频
        elif url.startswith('https://v.jin10.com/'):
            videoId = url.split('id=')[-1]
            videoPageUrl = 'https://v.jin10.com/datas/details/{}.json'.format(videoId)
            try:
                pro = GetProxy('http', self.proxies)
                resp = requests.get(videoPageUrl, self.videoPageHeaders, proxies=pro, verify=False, timeout=60)
                resp.encoding = 'utf-8'
                videoUrl = resp.json().get('video_url')
                if videoUrl:
                    try:
                        print("文章id {0} *** video Downloading ...... ***".format(pic_info['id']))
                        pro = GetProxy('http', self.proxies)
                        res = requests.get(videoUrl, proxies=pro, verify=False, timeout=60)
                        if res.status_code == 200:
                            basename = hashlib.md5(url.encode("utf8")).hexdigest() + '.mp4'
                            filename = os.path.join(videoPath + '/' + basename)
                            with open(filename, "wb") as f:
                                content = res.content
                                f.write(content)

                            # upload video
                            uploadUrl = 'http://27.150.182.135:8855/articleMaterials/attach/video'

                            postData = {
                                'files': (basename, open(filename, 'rb').read()),
                                'id': pic_info['id'],
                                'type': pic_info['pic_type']
                            }
                            encode_data = encode_multipart_formdata(postData)
                            postData = encode_data[0]

                            self.videoUploadHeaders['Content-Type'] = encode_data[1]
                            try:
                                print("文章id {0} *** video upLoading ...... ***".format(pic_info['id']))
                                resp = requests.post(url=uploadUrl, headers=self.videoUploadHeaders, data=postData, verify=False, timeout=120)

                                if resp.json().get('code') == '200' and resp.json().get('entity'):
                                    print("文章id {0} *** upload video successfully *** upload status {1}".format(pic_info['id'], resp.json().get('code')))
                                    self.videoTrueLink = {str(hashlib.md5(url.encode("utf8")).hexdigest()): resp.json().get('entity').get('pathUrl')}
                                elif resp.json().get('status') == '500' and 'DuplicateKey' in resp.json().get('exception'):
                                    print('DuplicateKey')
                                    pass
                                else:
                                    logger.warning(resp.json())
                            except requests.exceptions.ConnectionError:
                                if retry < 2:
                                    print('网络问题，重试中...')
                                    return self.DownloadPicture_Video(url, pic_info, retry+1)
                                else:pass
                            except Exception as error:
                                logger.warning(error)
                    except requests.exceptions.ConnectionError:
                        if retry < 2:
                            print('网络问题，重试中...')
                            return self.DownloadPicture_Video(url, pic_info, retry + 1)
                        else:
                            pass
                    except Exception as error:
                        logger.warning(error)
                        return None
                    return None
                else:pass
            except requests.exceptions.ConnectionError:
                print('网络问题，重试中...')
                return self.DownloadPicture_Video(url, pic_info)
            except Exception as error:
                logger.warning(error)

        else:pass

    # 清洗 文本 ASCII
    @staticmethod
    def illegal_char(s):
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
    def UploadArticle(self, info):
        # 上传文章
        try:
            serverUrl = 'https://zuiyouliao-prod.oss-cn-beijing.aliyuncs.com/zx/image/'

            if info.get('title_picture'):
                basename = hashlib.md5(info.get('title_picture').encode("utf8")).hexdigest() + '.' + info.get('title_picture').split('.')[-1]
                imageNew = serverUrl + basename
                info['title_picture'] = imageNew

            # upload
            articleUpload = 'http://27.150.182.135:8855/articleMaterials/add'

            dataJson = {
                'relatedImageUrls': [],
                'relatedLinkUrl': '',  # string相关链接
                'relatedVideoUrl': '',  # string相关视频
                'content': info.get('title_text'),  # string正文
                'createTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # string($date-time)创建时间
                'id': str(info.get('news_id')),  # string素材文章id
                'releaseTime': str(info.get('createTime')),  # string($date-time)发布时间
                'titleImageUrls': [info.get('title_picture')] if info.get('title_picture') else [],  # [...]
                'type': 3,  # integer($int32)2 资讯 3快讯
                'updateTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # string($date-time)更新时间
                'channelName': '',
                'tagName': ''
            }

            if info.get('about_link'):
                relatedImageUrls = []
                for link in info.get('about_link'):
                    if link.startswith('https://v.jin10.com/'):
                        videoLinkKey = str(hashlib.md5(link.encode("utf8")).hexdigest())
                        dataJson.update({
                            'relatedVideoUrl': self.videoTrueLink.get(videoLinkKey)
                        })
                    elif link.endswith('.jpg') or link.endswith('.png') or link.endswith('.pdf') or link.endswith('.gif'):
                        basename = hashlib.md5(info.get('title_picture').encode("utf8")).hexdigest() + '.' + info.get('title_picture').split('.')[-1]
                        imageNew = serverUrl + basename
                        relatedImageUrls.append(imageNew)
                    else:
                        dataJson.update({
                            'relatedLinkUrl': link
                        })
                dataJson.update({
                    'relatedImageUrls': relatedImageUrls
                })
            try:
                resp = requests.post(url=articleUpload, headers=self.articleHeaders, data=json.dumps(dataJson), timeout=60)
                if resp.json().get('ok'):
                    print("文章id {0} *** upload Article successfully *** upload status {1}".format(dataJson.get('id'), resp.json().get('code')))
                elif resp.json().get('status') == 500 and 'DuplicateKey' in resp.json().get('exception'):
                    print('DuplicateKey')
                    pass
                else:
                    logger.warning(resp.json())
            except requests.exceptions.ConnectionError:
                print('网络问题，重试中...')
                return self.UploadArticle(info)
            except Exception as error:
                logger.warning(error)
        except Exception as error:
            logger.warning(error)


if __name__ == '__main__':
    t = TopicPageNews()
    t.GetHomepageData()

    # 指定时间爬取
    # t.GetMorePageNews('2020-08-29 00:40:02')