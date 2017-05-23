#!/bin/bash
current_date=2016-03-31
incr=1
while [ $incr -lt 12 ]
do
    nohup hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/feature_train_sample.sql &
	sleep 60s
	echo $current_date,part$incr 
    current_date=$(date -d "$current_date +1 day" +%Y-%m-%d)
    incr=`expr $incr + 1`
done
