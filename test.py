from pymongo import MongoClient
import pandas as pd


# client = MongoClient('mongodb://readWrite:readWrite123456@120.48.21.244:27017/kuaishou')
# author_list_coll = client['kuaishou']['author_list']
#
# data_list = []
# for num, i in enumerate(author_list_coll.find({"contact_status" : 1})):
#     data_list.append({
#         'keyword': i['keyword'],
#         'kwaiId': i['kwaiId'],
#         'user_name': i['user_name'],
#         'verified': i['verified'],
#         'lanv_status': i['lanv_status'],
#         'contact': i['contact'],
#         'contact_status': i['contact_status'],
#         'expireTime': i['expireTime'],
#         'web_user_id': i['web_user_id']
#     })
#
# key_list = ['keyword', 'kwaiId', 'user_name', 'verified', 'lanv_status', 'contact', 'contact_status', 'expireTime', 'web_user_id']
# info = pd.DataFrame(data_list, columns=key_list)
# info.to_csv('./ks_contact_status_1.csv', index=False)
