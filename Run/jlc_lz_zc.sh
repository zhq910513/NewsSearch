#!/bin/sh

while [ 1 ]
sleep 60
time=$(date "+%Y-%m-%d %H:%M:%S")
do
        ht=$(date "+%H")
        h=`expr $ht \* 60`
        m=$(date "+%M")
        now=`expr $h + $m`
        if [ 599 -lt $now -a $now -lt 601 ] || [ 689 -lt $now -a $now -lt 691 ] || [ 779 -lt $now -a $now -lt 781 ] || [ 869 -lt $now -a $now -lt 871 ] || [ 959 -lt $now -a $now -lt 961 ]
        then
                for name in jinlianchuang_xh.py
                do
                        count=`ps -ef|grep $name|grep -v grep|wc -l`

                        if [ $count -eq 0 ]
                        then
                                echo ${time}    $name start process.....  >> /home/zyl/NewsSearch/Logs/jlz_run.log
                                nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/Quotation/$name >/dev/null 2>&1 &
                        else
                                echo ${time}    $name runing.....  >> /home/zyl/NewsSearch/Logs/jlz_run.log
                        fi
                done
        fi
done