#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-
import sys

sys.path.append("../../")
import configparser
import os
from os import path

import pymysql
from pymysql.err import IntegrityError

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

# 读ini文件
settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))
conf = configparser.ConfigParser()
conf.read(settingPath, encoding="utf-8")
settingPath = os.path.abspath(os.path.join(dh + r'/Settings.ini'))


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


def UpdateToMysql(conn, updateSql, updateData):
    cursor = conn.cursor()
    try:
        cursor.execute(updateSql % updateData)
        conn.commit()
        print(updateData)
    except Exception as error:
        print(error)
        conn.commit()


def run():
    conn = MySql()

    get_cursor = conn.cursor()
    sql = "select hashKey, prod_specifications FROM `lz_domestic_market_price` WHERE `prod_specifications` LIKE '%.0%'"
    get_cursor.execute(sql)

    for info in get_cursor.fetchall():
        hashKey = info[0]
        prod_specifications = str(info[1]).replace('.0', '')
        updateSql = "update lz_domestic_market_price set prod_specifications='%s' where hashKey='%s'"

        updateData = (
            prod_specifications if prod_specifications else '',
            hashKey
        )
        UpdateToMysql(conn, updateSql, updateData)

    get_cursor.close()


if __name__ == '__main__':
    run()
