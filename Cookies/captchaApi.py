#!/usr/bin/python3
# -*- coding:utf-8 -*-

import os, sys
import hashlib
import time
import json
import requests
import logging
from os import path

df = os.path.abspath(path.dirname(__file__))
dh = os.path.abspath(path.dirname(df))

logPath = os.path.abspath(os.path.join(dh + '/Logs/Captcha.log'))
logger = logging.getLogger(logPath)
fh = logging.FileHandler(logPath, mode='a+', encoding='utf-8')
fh.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

FATEA_PRED_URL = "http://pred.fateadm.com"

"""
    验证码类型及编号：
    10100	1位纯数字
    10200	2位纯数字
    10300	3位纯数字
    10400	4位纯数字
    10500	5位纯数字
    10600	6位纯数字
    10700	7位纯数字
    10800	8位纯数字
    10900	9位纯数字

    20100	1位纯英文
    20200	2位纯英文
    20300	3位纯英文
    20400	4位纯英文
    20500	5位纯英文
    20600	6位纯英文
    20700	7位纯英文
    20800	8位纯英文
    20900	9位纯英文

    30100	1位数字英文
    30200	2位数字英文
    30300	3位数字英文
    30400	4位数字英文
    30500	5位数字英文
    30600	6位数字英文
    30700	7位数字英文
    30800	8位数字英文
    30900	9位数字英文

    40100	1位汉字
    40200	2位汉字
    40300	3位汉字
    40400	4位汉字
    40500	5位汉字
    40600	6位汉字
    40700	7位汉字
    40800	8位汉字 
"""


class TmpObj:
    def __init__(self):
        self.value = None


class Rsp:
    def __init__(self):
        self.ret_code = -1
        self.cust_val = 0.0
        self.err_msg = "succ"
        self.pred_rsp = TmpObj()
        self.request_id = ''

    def ParseJsonRsp(self, rsp_data):
        if rsp_data is None:
            self.err_msg = "http request failed, get rsp Nil data"
            return
        jrsp = json.loads(rsp_data)
        self.ret_code = int(jrsp["RetCode"])
        self.err_msg = jrsp["ErrMsg"]
        self.request_id = jrsp["RequestId"]
        if self.ret_code == 0:
            rslt_data = jrsp["RspData"]
            if rslt_data is not None and rslt_data != "":
                jrsp_ext = json.loads(rslt_data)
                if "cust_val" in jrsp_ext:
                    data = jrsp_ext["cust_val"]
                    self.cust_val = float(data)
                if "result" in jrsp_ext:
                    data = jrsp_ext["result"]
                    self.pred_rsp.value = data


def CalcSign(pd_id, passwd, timestamp):
    md5 = hashlib.md5()
    md5.update((timestamp + passwd).encode())
    csign = md5.hexdigest()

    md5 = hashlib.md5()
    md5.update((pd_id + timestamp + csign).encode())
    csign = md5.hexdigest()
    return csign


def CalcCardSign(cardid, cardkey, timestamp, passwd):
    md5 = hashlib.md5()
    md5.update(passwd + timestamp + cardid + cardkey)
    return md5.hexdigest()


def HttpRequest(url, body_data, img_data=""):
    rsp = Rsp()
    post_data = body_data
    files = {
        'img_data': ('img_data', img_data)
    }
    header = {
        'User-Agent': 'Mozilla/5.0',
    }
    rsp_data = requests.post(url, post_data, files=files, headers=header)
    rsp.ParseJsonRsp(rsp_data.text)
    return rsp


# api 功能
class FateadmApi:
    # API接口调用类
    # 参数（appID，appKey，pdID，pdKey）
    def __init__(self, app_id, app_key, pd_id, pd_key):
        self.app_id = app_id
        if app_id is None:
            self.app_id = ""
        self.app_key = app_key
        self.pd_id = pd_id
        self.pd_key = pd_key
        self.host = FATEA_PRED_URL

    def SetHost(self, url):
        self.host = url

    # 查询余额
    # 参数：无
    # 返回值：
    #   rsp.ret_code：正常返回0
    #   rsp.cust_val：用户余额
    #   rsp.err_msg：异常时返回异常详情
    def QueryBalc(self):
        tm = str(int(time.time()))
        sign = CalcSign(self.pd_id, self.pd_key, tm)
        param = {
            "user_id": self.pd_id,
            "timestamp": tm,
            "sign": sign
        }
        url = self.host + "/api/custval"
        rsp = HttpRequest(url, param)
        if rsp.ret_code == 0:
            logger.warning(
                "query succ ret: {} cust_val: {} rsp: {} pred: {}".format(rsp.ret_code, rsp.cust_val, rsp.err_msg,
                                                                          rsp.pred_rsp.value))
        else:
            logger.warning("query failed ret: {} err: {}".format(rsp.ret_code, rsp.err_msg.encode('utf-8')))
        return rsp

    # 查询网络延迟
    # 参数：pred_type:识别类型
    # 返回值：
    #   rsp.ret_code：正常返回0
    #   rsp.err_msg： 异常时返回异常详情
    def QueryTTS(self, pred_type):
        tm = str(int(time.time()))
        sign = CalcSign(self.pd_id, self.pd_key, tm)
        param = {
            "user_id": self.pd_id,
            "timestamp": tm,
            "sign": sign,
            "predict_type": pred_type,
        }
        if self.app_id != "":
            #
            asign = CalcSign(self.app_id, self.app_key, tm)
            param["appid"] = self.app_id
            param["asign"] = asign
        url = self.host + "/api/qcrtt"
        rsp = HttpRequest(url, param)
        if rsp.ret_code == 0:
            logger.warning(
                "query rtt succ ret: {} request_id: {} err: {}".format(rsp.ret_code, rsp.request_id, rsp.err_msg))
        else:
            logger.warning("predict failed ret: {} err: {}".format(rsp.ret_code, rsp.err_msg.encode('utf-8')))
        return rsp

    # 识别验证码
    # 参数：pred_type:识别类型  img_data:图片的数据
    # 返回值：
    #   rsp.ret_code：正常返回0
    #   rsp.request_id：唯一订单号
    #   rsp.pred_rsp.value：识别结果
    #   rsp.err_msg：异常时返回异常详情
    def Predict(self, pred_type, img_data, head_info=""):
        tm = str(int(time.time()))
        sign = CalcSign(self.pd_id, self.pd_key, tm)
        param = {
            "user_id": self.pd_id,
            "timestamp": tm,
            "sign": sign,
            "predict_type": pred_type,
            "up_type": "mt"
        }
        if head_info is not None or head_info != "":
            param["head_info"] = head_info
        if self.app_id != "":
            #
            asign = CalcSign(self.app_id, self.app_key, tm)
            param["appid"] = self.app_id
            param["asign"] = asign
        url = self.host + "/api/capreg"
        files = img_data
        rsp = HttpRequest(url, param, files)
        if rsp.ret_code == 0:
            logger.warning("predict succ ret: {} request_id: {} pred: {} err: {}".format(rsp.ret_code, rsp.request_id,
                                                                                         rsp.pred_rsp.value,
                                                                                         rsp.err_msg))
        else:
            logger.warning("predict failed ret: {} err: {}".format(rsp.ret_code, rsp.err_msg))
            if rsp.ret_code == 4003:
                # lack of money
                logger.warning("cust_val <= 0 lack of money, please charge immediately")
        return rsp

    # 从文件进行验证码识别
    # 参数：pred_type;识别类型  file_name:文件名
    # 返回值：
    #   rsp.ret_code：正常返回0
    #   rsp.request_id：唯一订单号
    #   rsp.pred_rsp.value：识别结果
    #   rsp.err_msg：异常时返回异常详情
    def PredictFromFile(self, pred_type, file_name, head_info=""):
        with open(file_name, "rb") as f:
            data = f.read()
        return self.Predict(pred_type, data, head_info=head_info)

    # 识别失败，进行退款请求
    # 参数：request_id：需要退款的订单号
    # 返回值：
    #   rsp.ret_code：正常返回0
    #   rsp.err_msg：异常时返回异常详情
    #
    # 注意:
    #    Predict识别接口，仅在ret_code == 0时才会进行扣款，才需要进行退款请求，否则无需进行退款操作
    # 注意2:
    #   退款仅在正常识别出结果后，无法通过网站验证的情况，请勿非法或者滥用，否则可能进行封号处理
    def Justice(self, request_id):
        if request_id == "":
            #
            return
        tm = str(int(time.time()))
        sign = CalcSign(self.pd_id, self.pd_key, tm)
        param = {
            "user_id": self.pd_id,
            "timestamp": tm,
            "sign": sign,
            "request_id": request_id
        }
        url = self.host + "/api/capjust"
        rsp = HttpRequest(url, param)
        if rsp.ret_code == 0:
            logger.warning("justice succ ret: {} request_id: {} pred: {} err: {}".format(rsp.ret_code, rsp.request_id,
                                                                                         rsp.pred_rsp.value,
                                                                                         rsp.err_msg))
        else:
            logger.warning("justice failed ret: {} err: {}".format(rsp.ret_code, rsp.err_msg.encode('utf-8')))
        return rsp

    # 充值接口
    # 参数：cardid：充值卡号  cardkey：充值卡签名串
    # 返回值：
    #   rsp.ret_code：正常返回0
    #   rsp.err_msg：异常时返回异常详情
    def Charge(self, cardid, cardkey):
        tm = str(int(time.time()))
        sign = CalcSign(self.pd_id, self.pd_key, tm)
        csign = CalcCardSign(cardid, cardkey, tm, self.pd_key)
        param = {
            "user_id": self.pd_id,
            "timestamp": tm,
            "sign": sign,
            'cardid': cardid,
            'csign': csign
        }
        url = self.host + "/api/charge"
        rsp = HttpRequest(url, param)
        if rsp.ret_code == 0:
            logger.warning("charge succ ret: {} request_id: {} pred: {} err: {}".format(rsp.ret_code, rsp.request_id,
                                                                                        rsp.pred_rsp.value,
                                                                                        rsp.err_msg))
        else:
            logger.warning("charge failed ret: {} err: {}".format(rsp.ret_code, rsp.err_msg.encode('utf-8')))
        return rsp

    # 充值，只返回是否成功
    # 参数：cardid：充值卡号  cardkey：充值卡签名串
    # 返回值： 充值成功时返回0
    def ExtendCharge(self, cardid, cardkey):
        return self.Charge(cardid, cardkey).ret_code

    # 调用退款，只返回是否成功
    # 参数： request_id：需要退款的订单号
    # 返回值： 退款成功时返回0
    #
    # 注意:
    #    Predict识别接口，仅在ret_code == 0时才会进行扣款，才需要进行退款请求，否则无需进行退款操作
    # 注意2:
    #   退款仅在正常识别出结果后，无法通过网站验证的情况，请勿非法或者滥用，否则可能进行封号处理
    def JusticeExtend(self, request_id):
        return self.Justice(request_id).ret_code

    # 查询余额，只返回余额
    # 参数：无
    # 返回值：rsp.cust_val：余额
    def QueryBalcExtend(self):
        rsp = self.QueryBalc()
        return rsp.cust_val

    # 从文件识别验证码，只返回识别结果
    # 参数：pred_type;识别类型  file_name:文件名
    # 返回值： rsp.pred_rsp.value：识别的结果
    def PredictFromFileExtend(self, pred_type, file_name, head_info=""):
        rsp = self.PredictFromFile(pred_type, file_name, head_info)
        return rsp.pred_rsp

    # 识别接口，只返回识别结果
    # 参数：pred_type:识别类型  img_data:图片的数据
    # 返回值： rsp.pred_rsp.value：识别的结果
    def PredictExtend(self, pred_type, img_data, head_info=""):
        return self.Predict(pred_type, img_data, head_info)


# 调用 image：base64或绝对路径     captcha_type默认类型：数字英文
class ParseCaptcha:
    def __init__(self, image, captcha_type):
        self.Type = captcha_type
        self.image = image
        self.pd_id = "132923"
        self.pd_key = "PcH1l3ETQsXNcX/KcqUkuDRKxfPStTt1"
        self.app_id = "332923"
        self.app_key = "ZdGA5ky/WxqZPSgBBPMoebKx759W1rCU"

        self.api = FateadmApi(self.app_id, self.app_key, self.pd_id, self.pd_key)

    # 传入图片(base64)，返回识别结果
    def AnalysisImage_base64(self):
        info = self.api.PredictExtend(self.Type, self.image)
        return (info.request_id, info.pred_rsp.value)

    # 传入图片绝对路径，返回识别结果
    def AnalysisImage_abspath(self):
        info = self.api.PredictFromFile(self.Type, self.image)
        return (info.request_id, info.pred_rsp.value)

    # 识别失败，退款
    def DrawBack(self, request_id):
        return self.api.Justice(request_id)


if __name__ == "__main__":
    ## test
    pc = ParseCaptcha(r'G:\forum\public\test1.png', '20800')
    info = pc.AnalysisImage_abspath()
    if info[1] == 'argument':  # 需要你们使用时判断  是否成功，成功则pass，否则申请退款
        print('识别成功 -- %s' % info[1])
    else:
        print('识别失败， 退款中')
        pc.DrawBack(info[0])

