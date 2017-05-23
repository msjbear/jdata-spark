drop table dev.dev_temp_msj_risk_jdata_action_wide_table;
create table if not exists dev.dev_temp_msj_risk_jdata_action_wide_table as 
select DISTINCT action.user_id,	
	action.sku_id,	
	action.time	as action_time,	
	action.model_id,
	action.type as action_type,	
	action.cate as action_cate,	
	action.brand as action_brand,
	user.age,
	user.sex,
	user.user_lv_cd,
	user.user_reg_dt,
	product.attr1,	
	product.attr2,	
	product.attr3,
	product.cate,
	product.brand
from
(select * from dev.dev_temp_msj_risk_jdata_action where user_id !='user_id') action 
left outer join
(select * from dev.dev_temp_msj_risk_jdata_user where user_id !='user_id') user
on action.user_id = user.user_id
left outer join
(select * from dev.dev_temp_msj_risk_jdata_product where sku_id !='sku_id') product
on action.sku_id = product.sku_id