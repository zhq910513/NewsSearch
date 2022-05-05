#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

# 主函数加入这两行，将项目的根目录(webapp)的上级路径加入到系统PATH中
import configparser
import datetime
import subprocess
import sys

sys.path.append("../")
import pymysql
from dingtalkchatbot.chatbot import DingtalkChatbot
import os
from os import path
from pymongo import MongoClient
import time
from bson.objectid import ObjectId
import logging

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))
logPath = os.path.abspath(os.path.join(df + r'/Logs/report.log'))
settingPath = os.path.abspath(os.path.join(df + r'/Settings.ini'))

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

# TIME
today_time = time.strftime("%Y%m%d", time.localtime(time.time()))

yesterday_time = time.strftime("%Y%m%d", time.localtime(time.time() - 86400))


def object_id_from_datetime(from_datetime=None):
    if not from_datetime:
        from_datetime = datetime.datetime.now()
    return ObjectId.from_datetime(generation_time=from_datetime)


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


conn = MySql()
cursor = conn.cursor()


def jlc_xh():
    check_time = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    # 执行次数
    try:
        with open(r'/home/zyl/NewsSearch/Logs/jlz_run.log', 'r', encoding='utf-8') as f:
            logs = f.readlines()
    except:
        logs = []
    times = 0
    for log in logs:
        if check_time in log and 'jinlianchuang_xh' in log and 'start' in log:
            times += 1

    # 今日增量
    today_count_sql = """SELECT id FROM `prod_price`.`jlc_price_info` WHERE `dt` = '{}'""".format(today_time)
    cursor.execute(today_count_sql)
    conn.commit()
    today_data_count = len(list(cursor.fetchall()))

    # 昨日总量
    yesterday_count_sql = """SELECT id FROM `prod_price`.`jlc_price_info` WHERE `dt` = '{}'""".format(yesterday_time)
    cursor.execute(yesterday_count_sql)
    conn.commit()
    yesterday_data_count = len(list(cursor.fetchall()))

    # 较昨日浮动
    rate = int(today_data_count) - int(yesterday_data_count)

    return times, today_data_count, rate


def lz_sj():
    check_time = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    # 执行次数
    try:
        with open(r'/home/zyl/NewsSearch/Logs/jlz_run.log', 'r', encoding='utf-8') as f:
            logs = f.readlines()
    except:
        logs = []
    times = 0
    for log in logs:
        if check_time in log and 'longzhong_sj' in log and 'start' in log:
            times += 1

    # 今日增量
    today_count_sql_market = """SELECT id FROM `prod_price`.`lz_domestic_market_price` WHERE `dt` = '{}'""".format(
        today_time)
    cursor.execute(today_count_sql_market)
    conn.commit()
    today_market_count = len(list(cursor.fetchall()))

    today_count_sql_factory = """SELECT id FROM `prod_price`.`lz_factory_produce_price` WHERE `dt` = '{}'""".format(
        today_time)
    cursor.execute(today_count_sql_factory)
    conn.commit()
    today_factory_count = len(list(cursor.fetchall()))

    today_count_sql_inter = """SELECT id FROM `prod_price`.`lz_international_market_price` WHERE `dt` = '{}'""".format(
        today_time)
    cursor.execute(today_count_sql_inter)
    conn.commit()
    today_inter_count = len(list(cursor.fetchall()))

    today_count = today_market_count + today_factory_count + today_inter_count

    # 昨日总量
    yesterday_count_sql_market = """SELECT id FROM `prod_price`.`lz_domestic_market_price` WHERE `dt` = '{}'""".format(
        yesterday_time)
    cursor.execute(yesterday_count_sql_market)
    conn.commit()
    yesterday_market_count = len(list(cursor.fetchall()))

    yesterday_count_sql_factory = """SELECT id FROM `prod_price`.`lz_factory_produce_price` WHERE `dt` = '{}'""".format(
        yesterday_time)
    cursor.execute(yesterday_count_sql_factory)
    conn.commit()
    yesterday_factory_count = len(list(cursor.fetchall()))

    yesterday_count_sql_inter = """SELECT id FROM `prod_price`.`lz_international_market_price` WHERE `dt` = '{}'""".format(
        yesterday_time)
    cursor.execute(yesterday_count_sql_inter)
    conn.commit()
    yesterday_inter_count = len(list(cursor.fetchall()))

    yesterday_count = yesterday_market_count + yesterday_factory_count + yesterday_inter_count

    # 较昨日浮动
    rate = int(today_count) - int(yesterday_count)

    return times, today_count, rate


def zc_sj():
    check_time = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    # 执行次数
    try:
        with open(r'/home/zyl/NewsSearch/Logs/jlz_run.log', 'r', encoding='utf-8') as f:
            logs = f.readlines()
    except:
        logs = []
    times = 0
    for log in logs:
        if check_time in log and 'zhuochuang_sj' in log and 'start' in log:
            times += 1

    # 今日增量
    today_count_sql_market = """SELECT id FROM `prod_price`.`zc_domestic_market_price` WHERE `dt` = '{}'""".format(
        today_time)
    cursor.execute(today_count_sql_market)
    conn.commit()
    today_market_count = len(list(cursor.fetchall()))

    today_count_sql_factory = """SELECT id FROM `prod_price`.`zc_domestic_market_price` WHERE `dt` = '{}'""".format(
        today_time)
    cursor.execute(today_count_sql_factory)
    conn.commit()
    today_factory_count = len(list(cursor.fetchall()))

    today_count_sql_inter = """SELECT id FROM `prod_price`.`zc_domestic_market_price` WHERE `dt` = '{}'""".format(
        today_time)
    cursor.execute(today_count_sql_inter)
    conn.commit()
    today_inter_count = len(list(cursor.fetchall()))

    today_count = today_market_count + today_factory_count + today_inter_count

    # 昨日总量
    yesterday_count_sql_market = """SELECT id FROM `prod_price`.`zc_domestic_market_price` WHERE `dt` = '{}'""".format(
        yesterday_time)
    cursor.execute(yesterday_count_sql_market)
    conn.commit()
    yesterday_market_count = len(list(cursor.fetchall()))

    yesterday_count_sql_factory = """SELECT id FROM `prod_price`.`zc_domestic_market_price` WHERE `dt` = '{}'""".format(
        yesterday_time)
    cursor.execute(yesterday_count_sql_factory)
    conn.commit()
    yesterday_factory_count = len(list(cursor.fetchall()))

    yesterday_count_sql_inter = """SELECT id FROM `prod_price`.`zc_domestic_market_price` WHERE `dt` = '{}'""".format(
        yesterday_time)
    cursor.execute(yesterday_count_sql_inter)
    conn.commit()
    yesterday_inter_count = len(list(cursor.fetchall()))

    yesterday_count = yesterday_market_count + yesterday_factory_count + yesterday_inter_count

    # 较昨日浮动
    rate = int(today_count) - int(yesterday_count)

    return times, today_count, rate


def stock():
    today_check_time = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    yesterday_check_time = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))
    # 执行次数
    try:
        with open(r'/home/zyl/NewsSearch/Logs/stock_run.log', 'r', encoding='utf-8') as f:
            logs = f.readlines()
    except:
        logs = []
    times = 0
    for log in logs:
        if today_check_time in log and 'Stock' in log and 'start' in log:
            times += 1

    # 今日增量
    today_count_sql_clooy = """SELECT id FROM `prod_price`.`securities_price_info` WHERE `futures_code` = 'CL00Y' AND `dt_datetime` > '{} 00:00:00' AND `futures_jyzt` = '0'""".format(
        today_check_time)
    cursor.execute(today_count_sql_clooy)
    conn.commit()
    today_clooy_count = len(list(cursor.fetchall()))

    today_count_sql_booy = """SELECT id FROM `prod_price`.`securities_price_info` WHERE `futures_code` = 'B00Y' AND `dt_datetime` > '{} 00:00:00' AND `futures_jyzt` = '0'""".format(
        today_check_time)
    cursor.execute(today_count_sql_booy)
    conn.commit()
    today_booy_count = len(list(cursor.fetchall()))

    # 昨日总量
    yesterday_count_sql = """SELECT id FROM `prod_price`.`securities_price_info` WHERE `dt_datetime` >= '{0} 00:00:00' AND `dt_datetime` <= '{1} 23:59:59' AND `futures_jyzt` = '0'""".format(
        yesterday_check_time, yesterday_check_time)
    cursor.execute(yesterday_count_sql)
    conn.commit()
    yesterday_count = len(list(cursor.fetchall()))

    # 较昨日浮动
    rate = (today_clooy_count + today_booy_count) - yesterday_count

    return times, today_clooy_count, today_booy_count, rate


def zixun():
    client = MongoClient('mongodb://readWrite:readWrite123456@127.0.0.1:27017/Quotation')
    coll = client['Quotation']['upload']

    datetime_start = datetime.datetime(int(time.strftime("%Y", time.localtime(time.time()))),
                                       int(time.strftime("%m", time.localtime(time.time()))),
                                       int(time.strftime("%d", time.localtime(time.time()))), 0, 0, 0)
    id_start = object_id_from_datetime(datetime_start)

    # 今日增量
    today_data_count = coll.find({"_id": {"$gt": ObjectId("{}".format(id_start))}}).count()

    return today_data_count


def send_meaasge(retry=1):
    webhook = 'https://oapi.dingtalk.com/robot/send?' \
              'access_token=ae5ee6aad7142340e40194f90b0bfcfed510e568ccfb781e942e18deb7195ea2'

    try:
        jlc_xh_data = jlc_xh()
        lz_sj_data = lz_sj()
        zc_sj_data = zc_sj()
        stock_data = stock()
        zixun_data = zixun()
        message = DingtalkChatbot(webhook)

        if 21 <= int(time.strftime("%H", time.localtime(time.time()))) <= 23:
            message.send_markdown(title="SpiderStatus",
                                  text="Date: {0}\n\n"
                                       "------------------------------------\n\n\n"
                                       "  jlc_xh: 执行次数：{1}   今日增量：{2}   较昨日浮动：{3}\n\n\n"
                                       "  lz_sj: 执行次数{4}   今日增量：{5}   较昨日浮动：{6}\n\n\n"
                                       "  zc_sj: 执行次数{7}   今日增量：{8}   较昨日浮动：{9}\n\n\n"
                                       "  stock: 执行次数{10}   今日CLOOY增量：{11}   今日BOOY增量：{12}   较昨日浮动：{13}\n\n\n"
                                       "  资讯: 今日增量 >{14}\n\n\n"
                                  .format(
                                      today_time,
                                      jlc_xh_data[0], jlc_xh_data[1], jlc_xh_data[2],
                                      lz_sj_data[0], lz_sj_data[1], lz_sj_data[2],
                                      zc_sj_data[0], zc_sj_data[1], zc_sj_data[2],
                                      stock_data[0], stock_data[1], stock_data[2], stock_data[3],
                                      zixun_data
                                  ))
        else:
            print("Date: {0}\n\n"
                  "  jlc_xh: 执行次数：{1}   今日增量：{2}   较昨日浮动：{3}\n\n\n"
                  "  lz_sj: 执行次数{4}   今日增量：{5}   较昨日浮动：{6}\n\n\n"
                  "  zc_sj: 执行次数{7}   今日增量：{8}   较昨日浮动：{9}\n\n\n"
                  "  stock: 执行次数{10}   今日CLOOY增量：{11}   今日BOOY增量：{12}   较昨日浮动：{13}\n\n\n"
                  "  资讯: 今日增量 >{14}\n\n\n".format(
                today_time, jlc_xh_data[0], jlc_xh_data[1], jlc_xh_data[2],
                lz_sj_data[0], lz_sj_data[1], lz_sj_data[2], zc_sj_data[0],
                zc_sj_data[1], zc_sj_data[2], stock_data[0], stock_data[1],
                stock_data[2], stock_data[3],
                zixun_data
            ))
    except Exception as e:
        logging.warning(e)
        if retry > 0:
            return send_meaasge(retry - 1)
        else:
            logging.warning('The number of retries has been exhaustes !')
            pass


def kill_chrome_mitmproxy():
    for key in ['python3', 'chrome', 'mitmproxy']:
        cmd = r"ps -ef|grep " + key + "|awk '{print $2}'|xargs kill -9"
        subprocess.Popen(cmd, shell=True)
        time.sleep(1)


def run():
    send_meaasge(3)
    conn.cursor().close()
    time.sleep(5)
    # kill_chrome_mitmproxy()


if __name__ == '__main__':
    run()
