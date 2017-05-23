drop table dev.dev_temp_msj_risk_jdata_feature_user_train_v3_sample_${suffix};
create table dev.dev_temp_msj_risk_jdata_feature_user_train_v3_sample_${suffix} as
    select * from dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} where label = 1
    UNION ALL
	select * from dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} where label = 1
    UNION ALL
	select * from dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} where label = 1
    UNION ALL
	select * from dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} where label = 1
    UNION ALL
	select * from dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} where label = 1
    UNION ALL
	select * from
	(
		select * from dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} where label = 0 order by rand() limit 100000
	) tmp;
