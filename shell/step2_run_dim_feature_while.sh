#!/bin/bash
current_date=2016-03-31
incr=1
while [ $incr -lt 12 ]
do
    nohup hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_brand_feature_etl.sql &
    nohup hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_sku_feature_etl.sql &
    nohup hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_user_brand_feature_etl.sql &
    nohup hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_user_feature_etl.sql &
    nohup hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_user_sku_feature_etl.sql &
	sleep 60s
	echo $current_date,part$incr 
    current_date=$(date -d "$current_date +1 day" +%Y-%m-%d)
    incr=`expr $incr + 1`
done
