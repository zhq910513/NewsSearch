#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import os
import sys
try:
    from .captchaApi import ParseCaptcha
except:
    from captchaApi import ParseCaptcha

sys.path.append("../")
import configparser
import logging
import pprint
import re
import time
import subprocess
from os import path
from pymongo import MongoClient
from bs4 import BeautifulSoup
import pytesseract
import requests
from PIL import Image
from selenium import webdriver
from selenium.webdriver.common.by import By

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + r'/Logs/cookies.log'))
settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))
pictureFullPath = os.path.abspath(os.path.join(dh + r'/Cookies/full_screen.png'))
pictureCaptchaPath = os.path.abspath(os.path.join(dh + r'/Cookies/captcha.png'))

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


class Cookie:
    def __init__(self, usr):
        self.usr = usr
        self.platform = self.usr['platform']
        self.account = self.usr['account']
        self.pwd = self.usr['pwd']
        self.get_pid()
        self.StartCMD()
        db = conf.get("Mongo", "COOKIE")
        # client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/cookie')
        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/cookie')
        self.cookie_coll = client[db]['cookies']
        print("开始登录...")

        # 创建chrome参数对象
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')  # 解决DevToolsActivePort文件不存在的报错
        options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面
        options.add_argument(('--proxy-server=' + '127.0.0.1:65534'))

        self.executablePath = conf.get("Google", "EXECUTABLE_PATH")
        self.JinLianChuangUrl = 'http://member.315i.com/logreg/toIndex?gotourl=http%3A%2F%2Fwww.315i.cn%2F'
        self.ZhuoChuangUrl = 'https://www.sci99.com/'
        self.LongZhongUrl = 'https://dc.oilchem.net/'
        self.driver = webdriver.Chrome(options=options, executable_path=self.executablePath)
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
               Object.defineProperty(navigator, 'webdriver', {
                 get: () => false
               })
             """
        })
        self.driver.set_window_size(1920, 1080)
        print('driver启动成功...')
        self.captchaApi = None
        self.request_id = ''
        self.ChoicePlatform()

    @staticmethod
    def StartCMD():
        # cmd = r'mitmdump -p 65534 -q -s D:\Projects\NewsSearch\get_mitmproxy_response.py'
        cmd = r'mitmdump -p 65534 -q -s /home/zyl/NewsSearch/get_mitmproxy_response.py'
        subprocess.Popen(cmd, shell=True)
        time.sleep(10)

    # @staticmethod
    # def Account(platform):
    #     usrs = {
    #         'jlc': ['jinyang8', 'jinyang168'],
    #         'jlc_second': ['18918096272', '123456'],
    #         'zc': ['jinyang', 'jy123456'],
    #         'zc_second': ['dong135', 'htfc2017']
    #     }
    #     return usrs.get(platform)

    # 选择平台
    def ChoicePlatform(self):
        print(f'***选择平台: {self.platform}***')
        if 'jlc' in self.platform:
            self.JinLianChuangLogin()
        elif 'lz' in self.platform:
            self.LongZhongLogin()
        elif 'zc' in self.platform:
            self.ZhuoChuangLogin()
        else:
            print('没有该平台')

    # 金联创
    def JinLianChuangLogin(self):
        cookie_dict = dict()
        loginCookie = []

        self.driver.get(self.JinLianChuangUrl)
        time.sleep(5)
        print('当前链接:  {}'.format(self.driver.current_url))

        try:
            # 填写账户
            self.driver.find_element(By.XPATH, '//*[@id="username"]').send_keys(self.account)
            time.sleep(1)
            print('填写账户')

            # 填写密码
            self.driver.find_element(By.XPATH, '//*[@id="password"]').send_keys(self.pwd)
            time.sleep(1)
            print('填写密码')

            # 获取验证码
            captchaNum = self.ParseCaptcha()
            print('验证码： %s' % captchaNum)

            if captchaNum:
                # 填入验证码
                self.driver.find_element(By.XPATH, '//*[@id="Security"]').send_keys(captchaNum)

                # 登录
                time.sleep(5)
                self.driver.find_element(By.XPATH, '//*[@id="toLogin"]/div[10]/a').click()

                try:
                    time.sleep(3)
                    if self.driver.current_url != self.JinLianChuangUrl:
                        time.sleep(3)
                        self.driver.get(
                            'http://jiag.315i.com/price/newmain?productClassId=004001001&columnClassId=004001001001&dateColumnClassId=004001001001001&timeType=0')
                        time.sleep(5)
                        if self.driver.find_element(By.XPATH, '//*[@id="loginv"]/div[1]/a[1]').text == self.account:
                            print(f'金联创 {self.account} 登录成功！')
                            for Type in [
                                ('jlc', 'jlc_xh_downloadDetail', 'https://jiag.315i.com/price/historyData?itemIdStr=5769154a2d098e378988c502&startDate=2019-01-01&endDate=2022-05-06&columnid=5769154a2d098e378988c501&timeType=1'), # 3 day
                                ('jlc', 'jlc_pe_进出口数据_article', 'http://plas.315i.com/infodetail/i14377737_p004001001_c005010.html'),
                                ('jlc', 'pe_juyixi', 'http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html'),
                                ('jlc', 'jlc_search', 'http://plas.315i.com/infodetail/i14826098_p004001001_c004002.html'),
                                # ('jlc_second', 'jlc_second', 'http://plas.315i.com/infodetail/i14826098_p004001001_c004002.html'),
                                # ('jlc_third', 'jlc_third', 'http://plas.315i.com/infodetail/i14826098_p004001001_c004002.html')
                            ]:
                                if self.platform != Type[0]:
                                    continue

                                self.driver.get(Type[2])
                                print('%s' % Type[1])
                                if 'jlc_pe' in Type[1]:
                                    time.sleep(1)
                                    self.driver.refresh()
                                    time.sleep(5)
                                else:
                                    for cookie in self.driver.get_cookies():
                                        cookie_dict[cookie['name']] = cookie['value']
                                    for item in cookie_dict.items():
                                        loginCookie.append('{}={}'.format(item[0], item[1]))
                                    cookie = ';'.join(loginCookie)
                                    print(Type)

                                    # 存储cookie
                                    self.cookie_coll.update_one({'name': Type[1]}, {'$set': {
                                        'name': Type[1],
                                        'cookie': cookie,
                                        'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}}, upsert=True)

                            logger.warning(f'金联创 {self.account} cookie 获取成功！')
                        else:
                            logger.warning('金联创 %s 登录失败！' % self.driver.current_url)
                    else:
                        logger.warning(f'金联创 {self.account} 登录失败！-- {self.driver.current_url}')
                        return self.ChoicePlatform()
                except:
                    return self.ChoicePlatform()
        except Exception as error:
            logger.warning(error)

    # 隆众
    def LongZhongLogin(self):
        cookie_dict = dict()
        loginCookie = []

        self.driver.get(self.LongZhongUrl)
        time.sleep(5)

        try:
            # 点击登录按钮
            self.driver.find_element(By.XPATH, '//*[@id="header_menu_top_login"]/a[1]').click()
            time.sleep(1)

            # 填写账户
            self.driver.find_element(By.ID, 'dialogUsername').send_keys(self.account)
            time.sleep(1)

            # 填写密码
            self.driver.find_element(By.ID, 'dialogPassword').send_keys(self.pwd)
            time.sleep(1)

            # 获取验证码
            captchaNum = self.ParseCaptcha()
            print('验证码： %s' % captchaNum)

            if captchaNum:
                # 填入验证码
                self.driver.find_element(By.ID, 'dialogImgCodeStr').send_keys(captchaNum)

                # 登录
                time.sleep(3)
                self.driver.find_element(By.XPATH, '//*[@id="dialogForm"]/div[5]/button').click()

                try:
                    time.sleep(3)
                    if BeautifulSoup(self.driver.page_source,'lxml').find('div',{'class': 'plj'}):
                        print(f'隆众 {self.account} 登录成功！')
                        time.sleep(1)
                        # 获取 cookie
                        for Type in [
                            ('lz_sj_category', 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99'),
                            ('lz_sj_downloadDetail', 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99'),

                            ('lz_pe_国际装置_article', 'https://news.oilchem.net/21-0114-15-8e42d0c67a2fd9fa.html'),
                            ('lz_pe_聚乙烯开工率_article', 'https://www.oilchem.net/22-0310-17-a535003cb78800da.html'),
                            ('lz_pe_企业库存_article', 'https://news.oilchem.net/21-0122-08-25a8c8cb7531f3bd.html'),
                            ('lz_pe_港口库存_article', 'https://www.oilchem.net/22-0512-17-21dced5e5ffdc7bc.html'),
                            ('lz_pe_包装膜开工率_article', 'https://news.oilchem.net/21-0108-10-58c32bcd6821beb8.html'),
                            ('lz_pp_qita', 'https://www.oilchem.net/21-0122-16-a83c07bceee96550.html'),
                            ('lz_pe_qiye_article', 'https://news.oilchem.net/21-0416-11-2988086f0219d38f.html'),
                            ('lz_pp_gn_article', 'https://news.oilchem.net/21-0419-14-916425169ad531d9.html'),
                            ('lz_search', 'https://news.oilchem.net/21-0621-16-57057bf2f9acdcd8.html'),

                            # second
                            ('lz_second', 'https://news.oilchem.net/21-0621-16-57057bf2f9acdcd8.html')
                        ]:

                            self.driver.get(Type[1])
                            print('访问 -- %s' % Type[0])

                            time.sleep(10)
                            if BeautifulSoup(self.driver.page_source, 'lxml').find('div',{'class': 'plj'}):
                                if 'lz_pe' in Type[0] or 'lz_pe_qiye_article' in Type[0] or 'lz_pp_gn_article' in Type[0]:
                                    time.sleep(10)
                                    self.driver.refresh()
                                    time.sleep(5)
                                else:
                                    for cookie in self.driver.get_cookies():
                                        cookie_dict[cookie['name']] = cookie['value']
                                    for item in cookie_dict.items():
                                        loginCookie.append('{}={}'.format(item[0], item[1]))
                                    cookie = ';'.join(loginCookie)
                                    # print(Type[0], cookie, '\n')

                                    # 存储cookie
                                    self.cookie_coll.update_one({'name': Type[0]}, {
                                        '$set': {'name': Type[0], 'cookie': cookie,
                                                 'update_time': time.strftime("%Y-%m-%d %H:%M:%S",
                                                                              time.localtime(time.time()))}},
                                                                upsert=True)
                            else:
                                print(f'隆众 {self.account} 登录失败！ --- {self.driver.current_url}')
                                self.driver.refresh()
                                time.sleep(5)
                                continue
                        logger.warning(f'隆众 {self.account} cookie 获取成功！')
                    else:
                        self.captchaApi.DrawBack(self.request_id)
                        logger.warning(f'隆众 {self.account} 登录失败！-- {self.driver.current_url}')
                except Exception as error:
                    logger.warning(error)
            else:
                logger.warning('获取验证码失败')
        except Exception as error:
            logger.warning(error)

    # 卓创
    def ZhuoChuangLogin(self):
        cookie_dict = dict()
        loginCookie = []

        self.driver.get(self.ZhuoChuangUrl)
        time.sleep(5)

        try:
            self.driver.get('https://www.sci99.com//include/sciLogin.aspx')
            time.sleep(1)

            # 填写账户
            self.driver.find_element(By.XPATH, '//*[@id="chemname"]').send_keys(self.account)
            time.sleep(1)

            # 填写密码
            self.driver.find_element(By.XPATH, '//*[@id="chempwd"]').send_keys(self.pwd)
            time.sleep(1)

            # 登录
            time.sleep(3)
            self.driver.find_element(By.CSS_SELECTOR, '#IB_Login').click()

            try:
                time.sleep(3)
                if 'Hi,您好' in self.driver.find_element(By.XPATH, '//*[@id="divLogined"]/p/span').text:
                    print(f'卓创 {self.account} 登录成功！')
                    time.sleep(1)
                    # 获取 cookie
                    for Type in [
                        ('zc_sj_category', 'https://prices.sci99.com/cn/product.aspx?ppid=12278&ppname=LDPE&navid=521'),
                        ('zc_sj_downloadDetail', 'https://prices.sci99.com/cn/product_price.aspx?diid=39246&datatypeid=37&ppid=12278&ppname=LDPE&cycletype=day'),
                        ('zc_zs_category', 'https://index.sci99.com/channel/product/hy/%E5%A1%91%E6%96%99/3.html'),
                        ('zc_zs_downloadDetail', 'https://index.sci99.com/channel/product/hy/%E5%A1%91%E6%96%99/3.html'),
                        ('zc_pe_装置动态_article', 'https://plas.chem99.com/news/37614094.html'),
                        ('zc_pe_国内石化_article', 'https://plas.chem99.com/news/37307264.html'),
                        ('zc_pe_农膜日评_article', 'https://plas.chem99.com/news/38097594.html'),
                        ('zc_pe_塑膜收盘_article', 'https://plas.chem99.com/news/37294719.html'),
                        ('zc_pe_神华竞拍_article', 'https://plas.chem99.com/news/37528935.html'),
                        ('zc_pp_messages', 'https://www.sci99.com/search/?key=PP%E8%A3%85%E7%BD%AE%E5%8A%A8%E6%80%81%E6%B1%87%E6%80%BB&siteid=0'),
                        ('zc_pp_article', 'https://plas.chem99.com/news/37665388.html'),
                        ('zc_pp_bxxy_article', 'https://chem.chem99.com/news/36724085.html'),
                        ('zc_search', 'https://plas.chem99.com/news/38213547.html'),

                        # second
                        ('zc_sj_category_second', 'https://prices.sci99.com/cn/product.aspx?ppid=12555&ppname=%u518D%u751F%u9AD8%u538B&navid=552'),
                        ('zc_sj_downloadDetail_second', 'https://prices.sci99.com/cn/product_price.aspx?diid=80028&datatypeid=37&ppid=12555&ppname=%u518D%u751F%u9AD8%u538B&cycletype=day'),
                    ]:
                        self.driver.get(Type[1])
                        print('访问 -- %s' % Type[0])

                        time.sleep(5)

                        # 覆盖该类型cookie
                        if Type[0] == 'zc_zs_downloadDetail' or Type[0] == 'zc_pp_bxxy_article' or 'zc_pe' in Type[
                            0] or 'zc_pp_article' in Type[0] or '农膜日评' in Type[0]:
                            time.sleep(1)
                            self.driver.refresh()
                            time.sleep(5)
                        else:
                            for cookie in self.driver.get_cookies():
                                cookie_dict[cookie['name']] = cookie['value']
                            for item in cookie_dict.items():
                                loginCookie.append('{}={}'.format(item[0], item[1]))
                            cookie = ';'.join(loginCookie)
                            # print(Type[0], cookie, '\n')

                            # 存储cookie
                            self.cookie_coll.update_one({'name': Type[0]}, {'$set': {'name': Type[0], 'cookie': cookie,
                                                                                     'update_time': time.strftime(
                                                                                         "%Y-%m-%d %H:%M:%S",
                                                                                         time.localtime(time.time()))}},
                                                        upsert=True)

                    logger.warning(f'卓创 {self.account} cookie 获取成功！')
                else:
                    logger.warning(f'卓创 {self.account} 登录失败！-- {self.driver.current_url}')
                    return self.ChoicePlatform()
            except:
                return self.ChoicePlatform()
        except Exception as error:
            logger.warning(error)

    # 获取屏幕截图及验证码截图
    def GetScreenshot(self):
        time.sleep(2)
        """
            获取验证码截图
        """
        # 获取对应的坐标
        if 'jlc' in self.platform:
            url = self.JinLianChuangUrl
            # 无界面
            x2 = 1545
            y2 = 380
            x1 = 1450
            y1 = 334
        elif 'lz' in self.platform:
            url = self.LongZhongUrl
            # 无界面
            x2 = 1118
            y2 = 527
            x1 = 1030
            y1 = 497
            # 有界面
            # x2 = 833
            # y2 = 313
            # x1 = 746
            # y1 = 281
        else:
            url = None
            x2 = 0
            y2 = 0
            x1 = 0
            y1 = 0

        # 截图
        if url == self.driver.current_url:
            self.driver.save_screenshot(pictureFullPath)  # 一次截图：形成全图
            picture = Image.open(pictureFullPath)
            picture = picture.crop((x1, y1, x2, y2))  # 有界面二次截图：形成区块截图
            picture.save(pictureCaptchaPath)
            print('获取 验证码截图 成功')
        else:
            self.driver.refresh()
            self.ChoicePlatform()

    # 解析验证码图片
    def ParseCaptcha(self):
        # 获取验证码截图
        self.GetScreenshot()

        # 识别验证码
        time.sleep(3)
        if 'jlc' in self.platform:
            text = pytesseract.image_to_string(Image.open(pictureCaptchaPath), lang='eng')
            captcha = re.findall('\d\d\d\d', text, re.S)
            if captcha:
                return captcha[0]
            else:
                self.driver.refresh()
                self.ChoicePlatform()
        elif 'lz' in self.platform:
            # 调用验证码API
            self.captchaApi = ParseCaptcha(pictureCaptchaPath, '30400')
            captcha = self.captchaApi.AnalysisImage_abspath()
            if captcha:
                self.request_id = captcha[0]
                return captcha[1]
            else:
                return self.ParseCaptcha()
        else:
            pass

    @staticmethod
    def select_pid():
        status = False
        cmd = "ps -ef | grep 65534"
        pidList = [j for j in os.popen(cmd).read().split('root') if j]
        for info in pidList:
            if 'chrome' in info:
                status = True
                break
            else:
                status = False
        return status

    def get_pid(self):
        """通过端口获取pid"""
        if not self.select_pid():
            cmd = "ps -ef | grep 65534"
            pidList = [j for j in os.popen(cmd).read().split('root') if j]
            for info in pidList:
                pid = [i for i in info.split(' ') if i]
                if pid:
                    try:
                        self.kill_pid(pid[0])
                    except:
                        pass

    @staticmethod
    def kill_pid(pid):
        """通过pid杀死进程"""
        cmd = "kill -9 {}".format(pid)
        subprocess.Popen(cmd, shell=True)


class CookieSearch:
    def __init__(self):
        db = conf.get("Mongo", "COOKIE")
        client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/cookie')
        self.cookie_coll = client[db]['cookies']
        print("开始登录...")

        # 创建chrome参数对象
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')  # 解决DevToolsActivePort文件不存在的报错
        options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面

        self.executablePath = conf.get("Google", "EXECUTABLE_PATH")
        self.platform = ['zszx', 'ywcq']
        self.JinLianChuangUrl = 'http://member.315i.com/'
        self.ZhuoChuangUrl = 'https://www.sci99.com/'
        self.LongZhongUrl = 'https://dc.oilchem.net/'
        self.driver = webdriver.Chrome(options=options, executable_path=self.executablePath)
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
               Object.defineProperty(navigator, 'webdriver', {
                 get: () => false
               })
             """
        })
        self.driver.set_window_size(1920, 1080)
        print('driver启动成功...')
        self.captchaApi = ''
        self.request_id = ''
        self.ChoicePlatform()

    # 选择平台
    def ChoicePlatform(self):
        print('***选择平台:***')
        for platform in self.platform:
            print('***{}***'.format(platform))
            if platform == 'zszx':
                for info in [
                    ('zszx_1', 'https://info.21cp.com/info/column-list/160118142410981376-0-15-1.html'),
                    ('zszx_2', 'https://info.21cp.com/info/column-list/160118150547931136-0-15-1.html')
                ]:
                    self.PlatformCookies(info)
            elif platform == 'ywcq':
                for info in [
                    ('ywcq', 'https://cn.investing.com/news/commodities-news/article-2028202')
                ]:
                    self.PlatformCookies(info)
            else:
                print('没有该平台')

        self.driver.close()

    # 中塑在线/英为财情
    def PlatformCookies(self, info):
        cookie_dict = dict()
        loginCookie = []

        try:
            self.driver.get(info[1])
            print('访问 -- %s' % info[0])

            time.sleep(5)

            # cookie
            for cookie in self.driver.get_cookies():
                cookie_dict[cookie['name']] = cookie['value']
            for item in cookie_dict.items():
                loginCookie.append('{}={}'.format(item[0], item[1]))
            cookie = ';'.join(loginCookie)
            print(info[0], cookie, '\n')

            # 存储cookie
            self.cookie_coll.update_one({'name': info[0]}, {'$set': {'name': info[0], 'cookie': cookie,
                                                                     'update_time': time.strftime(
                                                                         "%Y-%m-%d %H:%M:%S",
                                                                         time.localtime(time.time()))}},
                                        upsert=True)
        except Exception as error:
            logger.warning(error)


def kill_chrome_mitmproxy():
    for key in ['chrome', 'mitmproxy']:
        cmd = f"ps -ef|grep {key}" + "|awk '{print $2}'|xargs kill -9"
        subprocess.Popen(cmd, shell=True)
        time.sleep(1)


def cookies_run():
    accounts = [
        #{'platform':'jlc', 'account': 'jinyang8', 'pwd': 'jinyang168'},
        #{'platform': 'jlc_second', 'account': '18918096272', 'pwd': '123456'},
        #{'platform': 'jlc_third', 'account': 'hshizhi', 'pwd': 'ZYLzyl@123@'},
        {'platform': 'lz', 'account': 'zhq111', 'pwd': 'a123456'},
        # {'platform': 'zc', 'account': 'changsu', 'pwd': 'cs123456'}
    ]
    for account in accounts:
        kill_chrome_mitmproxy()

        Cookie(account)

    # CookieSearch()


if __name__ == '__main__':
    cookies_run()