#!/bin/sh
cd /home/zyl/NewsSearch/Cookies

while [ 1 ]
sleep 60
time=$(date "+%Y-%m-%d %H:%M:%S")
do
        ht=$(date "+%H")
        h=`expr $ht \* 60`
        m=$(date "+%M")
        now=`expr $h + $m`
        if [ 579 -lt $now -a $now -lt 581 ] || [ 669 -lt $now -a $now -lt 651 ] || [ 759 -lt $now -a $now -lt 761 ] || [ 849 -lt $now -a $now -lt 851 ] || [ 939 -lt $now -a $now -lt 941 ] || [ 1059 -lt $now -a $now -lt 1061 ]
        then
                for name in GetCookie.py
                do
                        count=`ps -ef|grep $name|grep -v grep|wc -l`

                        if [ $count -eq 0 ]
                        then
                                echo ${time}    $name start process.....  >> /home/zyl/NewsSearch/Logs/cookies_run.log
                                nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/Cookies/$name >/dev/null 2>&1 &
                        else
                                echo ${time}    $name runing.....  >> /home/zyl/NewsSearch/Logs/cookies_run.log
                        fi
                done
        fi
done