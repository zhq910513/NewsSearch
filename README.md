# 线上服务器
    # 地址：27.150.182.135
    # 账户: root
    # 密码：BT@zyl@123
    # ssh -p 52315 root@27.150.182.135

# 系统详细版本
    # cat /etc/redhat-release

# python版本 3.6.5  /usr/local/python3

# 下载数据(excel/pdf)位置
    # /home/zyl/downloadData

# mongo 数据库
    # 安装位置 /usr/local/mongodb
    
    # 管理员账号
        # user: root
        # password: root123456
        # roles: root
        # db.createUser({user: 'root', pwd: 'root123456', roles: [{role: 'root', db: 'admin'}]})
        # user: userAdmin
        # password: root123456
        # roles: userAdminAnyDatabase
        # db.createUser({user: 'userAdmin', pwd: 'userAdmin123456', roles: [{role: 'userAdminAnyDatabase', db: 'admin'}]})
    
# 数据分类(数据库名)
    # 销售线索   SalesLeads
    # 物性库 PhysicalProperty
    # 视频   Videos
    # 资讯/快讯   RealTimeNewsFlash   zyl zyl123
    # 行情   Quotation   zyl zyl123

# 主数据库(详细表名)
    # db RealTimeNewsFlash
        # collections:
            ansou_topic
            jinshi_topic

            
    # db Quotation 
        # collections:               
            jlc_xh_category
            jlc_xh_categoryData
            
            lz_sj_category
            lz_sj_categoryData
            
            zc_sj_category
            zc_sj_categoryData
            zc_zs_category
            zc_zs_categoryData

            meiguo_category
            opec_category

            pp_zhuochuang_messages
            pp_zhuochuang_articleData hashKey
            
            pp_qita_messages
            pp_qita_articleData
            
            pe_wangye_messages
            pe_wangye_articleData


# 登录
    # ssh -p 52315 root@27.150.182.135

# dump数据
    # mongodump -h 127.0.0.1 -d zixun -c jlc_kx -o G:\
   
# unzip -O GBK/GB18030CP936

# 发送数据
    # scp -P 52315 G:/zixun.zip root@27.150.182.135:/home/zyl/downloadData

# restore
    # mongorestore -h 127.0.0.1 -u root -p root123456 --authenticationDatabase admin -d Quotation --dir /home/zyl/downloadData/zixun

# 定时文件 crontab
    # 0 0 * * * /usr/local/python3/bin/python3 /home/zyl/NewsSearch/run/RunRealTimeNewsFlash.py

    # 每分钟执行 * * * * *
    # 每小时执行 0 * * * *
    # 每天执行 0 0 * * *
    # 每周执行 0 0 * * 0
    # 每月执行 0 0 1 * *
    # 每年执行 0 0 1 1 *
    # 每小时的第3和第15分钟执行 3, 15 * * * *
