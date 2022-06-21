#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

import base64
import json
# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import os
import sys

# 一、图片文字类型(默认 3 数英混合)：
# 1 : 纯数字
# 1001：纯数字2
# 2 : 纯英文
# 1002：纯英文2
# 3 : 数英混合
# 1003：数英混合2
#  4 : 闪动GIF
# 7 : 无感学习(独家)
# 11 : 计算题
# 1005:  快速计算题
# 16 : 汉字
# 32 : 通用文字识别(证件、单据)
# 66:  问答题
# 49 :recaptcha图片识别
# 二、图片旋转角度类型：
# 29 :  旋转类型
#
# 三、图片坐标点选类型：
# 19 :  1个坐标
# 20 :  3个坐标
# 21 :  3 ~ 5个坐标
# 22 :  5 ~ 8个坐标
# 27 :  1 ~ 4个坐标
# 48 : 轨迹类型
#
# 四、缺口识别
# 18 : 缺口识别（需要2张图 一张目标图一张缺口图）
# 33 : 单缺口识别（返回X轴坐标 只需要1张图）
# 五、拼图识别
# 53：拼图识别


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

cmd = 'chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\selenium\AutomationProfile"'
# cmd = 'google-chrome --remote-debugging-port=9222 --user-data-dir="/home/zyl/selenium/AutomationProfile"'


def base64_api(uname, pwd, img, typeid):
    with open(img, 'rb') as f:
        base64_data = base64.b64encode(f.read())
        b64 = base64_data.decode()
    data = {"username": uname, "password": pwd, "typeid": typeid, "image": b64}
    result = json.loads(requests.post("http://api.ttshitu.com/predict", json=data).text)
    if result['success']:
        return result["data"]["result"]
    else:
        return result["message"]


def captcha_result(img_path, typeid):
    result = base64_api(uname='zhq996', pwd='Zhq951357', img=img_path, typeid=typeid)
    return result


class location_login:
    def __init__(self, usr):
        self.request_id = None
        self.captchaApi = None
        self.usr = usr
        self.platform = self.usr['platform']
        self.account = self.usr['account']
        self.pwd = self.usr['pwd']

        client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/cookie')
        self.cookie_coll = client['cookie']['cookies']
        self.start_cmd()

        options = webdriver.ChromeOptions()
        options.debugger_address = "127.0.0.1:9222"
        self.driver = webdriver.Chrome(options=options)

        self.jlc_check_url = 'http://member.315i.com/logreg/toIndex?gotourl=http%3A%2F%2Fwww.315i.cn%2F'
        self.zc_check_url = 'https://prices.sci99.com/cn/'
        self.lz_check_url = 'https://dc.oilchem.net/'

    @staticmethod
    def start_cmd():
        subprocess.Popen(cmd, shell=True)
        time.sleep(10)

    def lz_login(self):
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
            captchaNum = self.parse_captcha()
            print('验证码： %s' % captchaNum)

            if captchaNum:
                # 填入验证码
                self.driver.find_element(By.ID, 'dialogImgCodeStr').send_keys(captchaNum)

                time.sleep(1)
                self.driver.find_element(By.XPATH, '//*[@id="dialogRemberId"]').click()

                # 登录
                time.sleep(3)
                self.driver.find_element(By.XPATH, '//*[@id="dialogForm"]/div[5]/button').click()

                try:
                    time.sleep(3)
                    if BeautifulSoup(self.driver.page_source, 'lxml').find('div', {'class': 'plj'}):
                        return True
                    else:
                        self.captchaApi.DrawBack(self.request_id)
                        logger.warning(f'隆众 {self.account} 登录失败！-- {self.driver.current_url}')
                        return False
                except Exception as error:
                    logger.warning(error)
            else:
                logger.warning('获取验证码失败')
                return False
        except Exception as error:
            logger.warning(error)

    def zc_sci99_login(self):
        try:
            self.driver.get('https://my.sci99.com/sso/login.aspx')
            time.sleep(3)

            # 填写账户
            self.driver.find_element(By.XPATH, '//*[@id="SciName"]').send_keys(self.account)
            time.sleep(1)

            # 填写密码
            self.driver.find_element(By.XPATH, '//*[@id="SciPwd"]').send_keys(self.pwd)
            time.sleep(1)

            # 登录
            time.sleep(3)
            self.driver.find_element(By.XPATH, '//*[@id="Btn_Login"]').click()

            time.sleep(3)
            self.driver.get('https://prices.sci99.com/cn/include/header.aspx')
            time.sleep(3)
            if '欢迎您！' in str(self.driver.page_source) or self.account in str(self.driver.page_source):
                return True
            else:
                logger.warning(f'卓创 {self.account} 登录失败！-- {self.driver.current_url}')
                return False
        except Exception as error:
            logger.warning(error)

    def zc_chem99_login(self):
        try:
            self.driver.get('https://plas.chem99.com/')
            time.sleep(3)

            if '欢迎您！' in str(self.driver.page_source) or self.account in str(self.driver.page_source):
                return True
            else:
                try:
                    print('---------- chem99 没有登录 ----------')
                    self.driver.switch_to.frame(self.driver.find_elements(By.TAG_NAME, "iframe")[0])

                    # 填写账户
                    self.driver.find_element(By.XPATH, '//*[@id="SciName"]').send_keys(self.account)
                    time.sleep(1)

                    # 填写密码
                    self.driver.find_element(By.XPATH, '//*[@id="SciPwd"]').send_keys(self.pwd)
                    time.sleep(1)

                    # 登录
                    time.sleep(3)
                    self.driver.find_element(By.XPATH, '//*[@id="IB_Login"]').click()

                    time.sleep(3)
                    self.driver.refresh()
                    time.sleep(3)
                    if '欢迎您！' in str(self.driver.page_source) or self.account in str(self.driver.page_source):
                        return True
                    else:
                        logger.warning(f'卓创 {self.account} 登录失败！-- {self.driver.current_url}')
                        return False
                except Exception as error:
                    logger.warning(error)
        except Exception as error:
            logger.warning(error)

    def login(self):
        if 'lz' == self.platform:
            try:
                self.driver.get(self.lz_check_url)
                time.sleep(3)

                if BeautifulSoup(self.driver.page_source, 'lxml').find('div', {'class': 'plj'}):
                    return True
                else:
                    return self.lz_login()
            except Exception as error:
                logger.warning(error)

        elif 'zc' == self.platform:
            try:
                time.sleep(3)
                self.driver.get('https://prices.sci99.com/cn/include/header.aspx')
                time.sleep(3)

                if '欢迎您！' in str(self.driver.page_source) or self.account in str(self.driver.page_source):
                    return True
                else:
                    return self.zc_sci99_login()
            except Exception as error:
                logger.warning(error)

        else:
            print('暂时未添加该平台')
            return False

    def get_screen_shot(self):
        time.sleep(2)
        """
            获取验证码截图
        """
        # 获取对应的坐标
        if 'jlc' in self.platform:
            url = self.jlc_check_url
            # 无界面
            x2 = 1545
            y2 = 380
            x1 = 1450
            y1 = 334
        elif 'lz' in self.platform:
            url = self.lz_check_url
            # 无界面
            # x2 = 1118
            # y2 = 527
            # x1 = 1030
            # y1 = 497
            # 有界面
            x1 = 528
            y1 = 419
            x2 = 610
            y2 = 444
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

    def parse_captcha(self):
        # 获取验证码截图
        self.get_screen_shot()

        # 识别验证码
        time.sleep(3)
        if 'jlc' in self.platform:
            text = pytesseract.image_to_string(Image.open(pictureCaptchaPath), lang='eng')
            captcha = re.findall('\d\d\d\d', text, re.S)
            if captcha:
                return captcha[0]
            else:
                self.driver.refresh()
        elif 'lz' in self.platform:
            # 调用验证码API
            captcha = captcha_result(pictureCaptchaPath, '3')
            if captcha:
                return captcha
            else:
                return self.parse_captcha()
        else:
            pass

    def get_cookies(self):
        if not self.login():
            print(f'{self.platform}账户未登录')
            return

        # 隆众
        if 'lz' in self.platform:
            for Type in [
                ('lz_sj_category',
                 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99'),
                ('lz_sj_downloadDetail',
                 'https://dc.oilchem.net/price_search/list.htm?businessType=2&varietiesName=HDPE&varietiesId=313&templateType=6&flagAndTemplate=2-7;1-6;3-4&channelId=1776&oneName=%E5%A1%91%E6%96%99&twoName=%E9%80%9A%E7%94%A8%E5%A1%91%E6%96%99'),

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
                ('lz_second', 'https://news.oilchem.net/21-0621-16-57057bf2f9acdcd8.html')]:
                self.driver.get(Type[1])
                time.sleep(3)
                print(f'隆众登录成功 获取 {Type[0]}')
                self.save_cookies(Type)

        # 卓创
        if 'zc' in self.platform:
            for Type in [
                ('zc_sj_category', 'https://prices.sci99.com/cn/product.aspx?ppid=12278&ppname=LDPE&navid=521'),
                ('zc_sj_downloadDetail',
                 'https://prices.sci99.com/cn/product_price.aspx?diid=39246&datatypeid=37&ppid=12278&ppname=LDPE&cycletype=day'),
                ('zc_zs_category', 'https://index.sci99.com/channel/product/hy/%E5%A1%91%E6%96%99/3.html'),
                ('zc_zs_downloadDetail', 'https://index.sci99.com/channel/product/hy/%E5%A1%91%E6%96%99/3.html'),
                ('zc_search', 'https://www.sci99.com/search/?key=LDPE&siteid=0'),
                ('zc_pp_messages', 'https://www.sci99.com/search/?key=PP%E8%A3%85%E7%BD%AE%E5%8A%A8%E6%80%81%E6%B1%87%E6%80%BB&siteid=0'),

                ('zc_pe_装置动态_article', 'https://plas.chem99.com/news/41765441.html'),
                ('zc_pe_国内石化_article', 'https://plas.chem99.com/news/41725745.html'),
                ('zc_pe_农膜日评_article', 'https://plas.chem99.com/news/41727334.html'),
                ('zc_pe_塑膜收盘_article', 'https://plas.chem99.com/news/41727049.html'),
                ('zc_pe_神华竞拍_article', 'https://plas.chem99.com/news/41770856.html'),

                ('zc_pp_article', 'https://plas.chem99.com/news/41775116.html'),
                # ('zc_pp_bxxy_article', 'https://chem.chem99.com/news/36724085.html'),
                # second
                # ('zc_sj_category_second', 'https://prices.sci99.com/cn/product.aspx?ppid=12555&ppname=%u518D%u751F%u9AD8%u538B&navid=552'),
                # ('zc_sj_downloadDetail_second', 'https://prices.sci99.com/cn/product_price.aspx?diid=80028&datatypeid=37&ppid=12555&ppname=%u518D%u751F%u9AD8%u538B&cycletype=day'),
            ]:
                if 'plas.chem99.com' in Type[1]:
                    if not self.zc_chem99_login():
                        return self.get_cookies()

                self.driver.get(Type[1])
                time.sleep(3)
                print(f'卓创登录成功 获取 {Type[0]}')
                self.save_cookies(Type)

        self.driver.close()

    def save_cookies(self, Type):
        cookie_dict = dict()
        loginCookie = []
        for cookie in self.driver.get_cookies():
            cookie_dict[cookie['name']] = cookie['value']
        for item in cookie_dict.items():
            loginCookie.append('{}={}'.format(item[0], item[1]))
        cookie = ';'.join(loginCookie)

        self.cookie_coll.update_one({'name': Type[0]}, {'$set': {
            'name': Type[0],
            'url': Type[1],
            'cookie': cookie,
            'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        }}, upsert=True)


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
    # linux
    try:
        for key in ['chrome', 'mitmproxy']:
            cmd = f"ps -ef|grep {key}" + "|awk '{print $2}'|xargs kill -9"
            subprocess.Popen(cmd, shell=True)
            time.sleep(1)
    except:
        pass

    # windows
    try:
        cmd = 'tasklist -v'
        info = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        for txt in str(info.stdout.read()).split('\\r\\n'):
            if 'chromedriver.exe' in txt or 'chrome.exe' in txt:
                pid_list = re.findall(' (\d+) Console ', txt, re.S)
                if pid_list:
                    cmd = "taskkill -f -pid {}".format(pid_list[0])
                    subprocess.Popen(cmd, shell=True)
    except:
        pass


def cookies_run():
    accounts = [
        # {'platform':'jlc', 'account': 'jinyang8', 'pwd': 'jinyang168'},
        # {'platform': 'jlc_second', 'account': '18918096272', 'pwd': '123456'},
        # {'platform': 'jlc_third', 'account': 'hshizhi', 'pwd': 'ZYLzyl@123@'},
        # {'platform': 'lz', 'account': 'zhq111', 'pwd': 'a123456'},
        {'platform': 'zc', 'account': 'changsu', 'pwd': 'cs123456'}
    ]
    for account in accounts:
        kill_chrome_mitmproxy()

        ll = location_login(account)
        ll.get_cookies()

        kill_chrome_mitmproxy()

    # CookieSearch()


if __name__ == '__main__':
    cookies_run()
