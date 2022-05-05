#!/bin/sh
cd /home/zyl/NewsSearch/Quotation
while [ 1 ]
do
        sleep 60
        for name in Stock.py
        do
                count=`ps -ef|grep $name|grep -v grep|wc -l`
                time=$(date "+%Y-%m-%d %H:%M:%S")

                if [ $count -eq 0 ]
                then
                        echo ${time}    $name start process.....  >> /home/zyl/NewsSearch/Logs/stock_run.log
                        nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/Quotation/$name >/dev/null 2>&1 &
                else
                        echo ${time}    $name runing.....  >> /home/zyl/NewsSearch/Logs/stock_run.log
                fi
        done

done

