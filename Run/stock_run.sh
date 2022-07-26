#!/bin/sh
cd /home/zyl/NewsSearch/Quotation

while [ 1 ]
sleep 60
do
    for name in stock.py
    do
        count=`ps -ef|grep $name|grep -v grep|wc -l`

        if [ $count -eq 0 ]
        then
                echo ${time}    $name start process.....  >> /home/zyl/NewsSearch/Logs/stock.log
                nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/Quotation/$name >/dev/null 2>&1 &
        else
                echo ${time}    $name runing.....  >> /home/zyl/NewsSearch/Logs/stock.log
        fi
    done
done
