#!/bin/sh
cd /home/zyl/NewsSearch/Quotation

while [ 1 ]
sleep 60
time=$(date "+%Y-%m-%d %H:%M:%S")
do
        ht=$(date "+%H")
        h=`expr $ht \* 60`
        m=$(date "+%M")
        now=`expr $h + $m`
        if [ 1079 -lt $now -a $now -lt 1081 ]
        then
                for name in longzhong_sj.py
                do
                        count=`ps -ef|grep $name|grep -v grep|wc -l`

                        if [ $count -eq 0 ]
                        then
                                echo ${time}    $name start process.....  >> /home/zyl/NewsSearch/Logs/longzhong_sj.log
                                nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/Quotation/$name >/dev/null 2>&1 &
                        else
                                echo ${time}    $name runing.....  >> /home/zyl/NewsSearch/Logs/longzhong_sj.log
                        fi
                done
        fi
done