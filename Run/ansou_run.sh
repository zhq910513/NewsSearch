#!/bin/sh
cd /home/zyl/NewsSearch/RealTimeNewsFlash
while [ 1 ]
do
        sleep 3600
        for name in ansou_news.py
        do
                count=`ps -ef|grep $name|grep -v grep|wc -l`
                time=$(date "+%Y-%m-%d %H:%M:%S")

                if [ $count -eq 0 ]
                then
                        echo ${time}    $name start process.....  >> run.log
                        nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/RealTimeNewsFlash/$name >/dev/null 2>&1 &
                else
                        echo ${time}    $name runing.....  >> run.log
                fi
        done

done

