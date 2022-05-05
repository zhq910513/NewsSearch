#!/usr/local/python3/bin/python3
# -*- coding:utf-8 -*-

import os
import sys
import time
from os import path
sys.path.append("../")

import pprint
from pymongo import MongoClient
pp=pprint.PrettyPrinter(indent=4)

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))
print('准备监测...')

def response(flow):
    # zc-zs
    if "https://index.sci99.com/api/zh-cn/dataitem/datavalue" in flow.request.url:
        print('准备捕获 -- zc_zs_downloadDetail')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_zs_downloadDetail', headers.get('cookie'))
                    SaveJosnData('zc_zs_downloadDetail', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    # zc-pp-article
    if "https://plas.chem99.com/news/37665388.html" in flow.request.url:
        print('准备捕获 -- zc_pp_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pp_article', headers.get('cookie'))
                    SaveJosnData('zc_pp_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    # zc-pp-bxxy
    if "https://chem.chem99.com/news/36724085.html" in flow.request.url:
        print('准备捕获 -- zc_pp_bxxy_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pp_bxxy_article', headers.get('cookie'))
                    SaveJosnData('zc_pp_bxxy_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    # zc-pe
    if "https://plas.chem99.com/news/37614094.html" in flow.request.url:
        print('准备捕获 -- zc_pe_装置动态_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pe_装置动态_article', headers.get('cookie'))
                    SaveJosnData('zc_pe_装置动态_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://plas.chem99.com/news/37307264.html" in flow.request.url:
        print('准备捕获 -- zc_pe_国内石化_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pe_国内石化_article', headers.get('cookie'))
                    SaveJosnData('zc_pe_国内石化_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://plas.chem99.com/news/37307908.html" in flow.request.url:
        print('准备捕获 -- zc_pe_农膜日评_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pe_农膜日评_article', headers.get('cookie'))
                    SaveJosnData('zc_pe_农膜日评_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://plas.chem99.com/news/37294719.html" in flow.request.url:
        print('准备捕获 -- zc_pe_塑膜收盘_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pe_塑膜收盘_article', headers.get('cookie'))
                    SaveJosnData('zc_pe_塑膜收盘_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://plas.chem99.com/news/37528935.html" in flow.request.url:
        print('准备捕获 -- zc_pe_神华竞拍_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pe_神华竞拍_article', headers.get('cookie'))
                    SaveJosnData('zc_pe_神华竞拍_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass


    # jlc-pe
    if "http://plas.315i.com/infodetail/i14377737_p004001001_c005010.html" in flow.request.url:
        print('准备捕获 -- jlc_pe_进出口数据_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('jlc_pe_进出口数据_article', headers.get('cookie'))
                    SaveJosnData('jlc_pe_进出口数据_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass


    # lz-pe
    if "https://news.oilchem.net/21-0114-15-8e42d0c67a2fd9fa.html" in flow.request.url:
        print('准备捕获 -- lz_pe_国际装置_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pe_国际装置_article', headers.get('cookie'))
                    SaveJosnData('lz_pe_国际装置_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://news.oilchem.net/21-0114-15-9af679cea3892e8d.html" in flow.request.url:
        print('准备捕获 -- lz_pe_聚乙烯开工率_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pe_聚乙烯开工率_article', headers.get('cookie'))
                    SaveJosnData('lz_pe_聚乙烯开工率_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://news.oilchem.net/21-0122-08-25a8c8cb7531f3bd.html" in flow.request.url:
        print('准备捕获 -- lz_pe_企业库存_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pe_企业库存_article', headers.get('cookie'))
                    SaveJosnData('lz_pe_企业库存_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://news.oilchem.net/21-0118-08-6cceb48bb4e3b2f7.html" in flow.request.url:
        print('准备捕获 -- lz_pe_港口库存_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pe_港口库存_article', headers.get('cookie'))
                    SaveJosnData('lz_pe_港口库存_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://news.oilchem.net/21-0108-10-58c32bcd6821beb8.html" in flow.request.url:
        print('准备捕获 -- lz_pe_包装膜开工率_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pe_包装膜开工率_article', headers.get('cookie'))
                    SaveJosnData('lz_pe_包装膜开工率_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://news.oilchem.net/21-0416-11-2988086f0219d38f.html" in flow.request.url:
        print('准备捕获 -- lz_pe_qiye_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pe_qiye_article', headers.get('cookie'))
                    SaveJosnData('lz_pe_qiye_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://news.oilchem.net/21-0419-14-916425169ad531d9.html" in flow.request.url:
        print('准备捕获 -- lz_pp_gn_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('lz_pp_gn_article', headers.get('cookie'))
                    SaveJosnData('lz_pp_gn_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

    if "https://plas.chem99.com/news/38097594.html" in flow.request.url:
        print('准备捕获 -- zc_pe_农膜日评_article')
        headers = flow.request.headers
        if headers:
            try:
                if headers.get('cookie'):
                    print('zc_pe_农膜日评_article', headers.get('cookie'))
                    SaveJosnData('zc_pe_农膜日评_article', headers.get('cookie'))
                else:
                    pass
            except:
                pass

def SaveJosnData(name, data):
    client = MongoClient('mongodb://readWrite:readWrite123456@27.150.182.135:27017/cookie')
    coll = client['cookie']['cookies']

    # 更新对应账户
    coll.update_one({'name': name}, {'$set': {'name': name, 'cookie': data, 'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}}, upsert=True)
    print('cookie写入完成！')

