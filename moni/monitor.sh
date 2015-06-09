#!/bin/bash
while [[ true  ]]; do
    num=`mysql -h 192.168.0.160 -uroot -p'OcnuRW_B+>' -D oj -N -e "select count(result) from solution where result=0;"` 
    echo "`date +%y-%m-%d/%H:%M:%S` waiting数:" $num
    if [[ $num -ne 0 ]]; then  
        echo "`date +%y-%m-%d/%H:%M:%S` oj在waiting了，尝试重启"
        cd /home/acmxs/oj_judge/ && sh start.sh
        sleep 60
        num=`mysql -h 192.168.0.160 -uroot -p'OcnuRW_B+>' -D oj -N -e "select count(result) from solution where result=0;"` 
    fi
    sleep 60
done
