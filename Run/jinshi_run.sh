#!/bin/sh

while [ 1 ]
sleep 60
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} 等待完毕"  >> run.log
do
        sleep 5400
        ht=$(date "+%H")
        h=`expr $ht \* 60`
        m=$(date "+%M")
        now=`expr $h + $m`
        if [ 510 -lt $now -a $now -lt 930 ]
        then
                for name in jinlianchuang_xh.py longzhong_sj.py
                do
                        count=`ps -ef|grep $name|grep -v grep|wc -l`

                        if [ $count -eq 0 ]
                        then
                                echo ${time}    $name start process.....  >> run.log
                                nohup /usr/local/python3/bin/python3  /home/zyl/NewsSearch/Quotation/$name >/dev/null 2>&1 &
                        else
                                echo ${time}    $name runing.....  >> run.log
                        fi
                done
        else
                echo "未到时间"  >> run.log
        fi
done