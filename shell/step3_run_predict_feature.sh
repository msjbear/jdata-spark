#!/bin/bash
current_date=2016-04-15
incr=15
hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_brand_feature_etl.sql 
hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_sku_feature_etl.sql 
hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_user_brand_feature_etl.sql 
hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_user_feature_etl.sql 
hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/dim_user_sku_predict_feature_etl.sql 

sleep 60s
hive -hivevar suffix=part${incr} -hivevar current_date=${current_date} -f ../script/dim_feature_v3/feature_predict_wide_table.sql

	
