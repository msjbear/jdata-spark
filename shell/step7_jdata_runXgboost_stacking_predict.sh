#!/bin/sh
numExecutor=50
executorMemory=20
driverMemory=10
etlDate=20160406
taskType=predict
spark-submit  \
--master  yarn-cluster \
--name  "SparkModelStackingMain" \
--class com.sjmei.jdata.xgboost.SparkModelStackingMain \
--properties-file ../conf/jdata/spark-defaults-jdata.conf \
--num-executors ${numExecutor}  \
--executor-memory ${executorMemory}g  \
--driver-memory ${driverMemory}g   \
--jars ../target/scopt_2.11-3.5.0.jar,../target/xgboost4j-0.7-jar-with-dependencies.jar,../target/xgboost4j-spark-0.7.jar,../target/velocity-1.7.jar \
--queue bdp_jmart_risk.bdp_jmart_risk_formal \
--files ../conf/graph-jdata.properties,../conf/hive-site.xml,../script/mid_result_script/model_blend_feature.sql,../script/mid_result_script/model_blend_pred_feature.sql,../script/mid_result_script/gen_submission_result.sql \
../target/jdata-spark-1.0-SNAPSHOT.jar  \
--nWorkers 50 \
--initDate 2016-04-11 \
hdfs://ns2/user/mart_risk/dev.db/dev_temp_msj_risk_jdata_feature_predict_v3 \
hdfs://ns2/user/mart_risk/sjmei/tests/xgboost/model_stacking_v3_${etlDate} \
hdfs://ns2/user/mart_risk/sjmei/tests/xgboost/result_stacking_v3_${etlDate} \
${taskType} \
>./jdata_runXgboost_blend_${taskType}.`date +%Y%m%d`.log 2>&1
