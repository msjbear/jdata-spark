drop table dev.dev_temp_msj_risk_jdata_feature_sku_dim_v3_${suffix};
create table if not exists dev.dev_temp_msj_risk_jdata_feature_sku_dim_v3_${suffix} stored as orc as select
	sku_month_action.*,
	comments.`(sku_id)?+.+`,
	product.`(sku_id)?+.+`,
	sku_1days_action.`(sku_id)?+.+`,
	sku_3days_action.`(sku_id)?+.+`,
	sku_5days_action.`(sku_id)?+.+`,
	sku_7days_action.`(sku_id)?+.+`,
	sku_10days_action.`(sku_id)?+.+`,
	sku_15days_action.`(sku_id)?+.+`
from
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_month_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_month_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_month_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_month_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_month_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_month_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_month_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_month_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_month_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_month_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_month_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_month_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_month_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_month_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_month_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_month_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_month_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_month_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_month_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_month_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_month_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_month_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_month_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_month_focus_buy_user_rate,
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_month_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_month_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end) as sku_month_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end) as sku_month_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_month_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_month_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/30 as sku_month_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/30 as sku_month_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/30 as sku_month_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/30 as sku_month_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/30 as sku_month_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/30 as sku_month_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/30 as sku_month_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/30 as sku_month_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/30 as sku_month_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/30 as sku_month_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/30 as sku_month_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/30 as sku_month_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',30) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by sku_id
) sku_month_action
left outer join
(
	select sku_id,comment_num,has_bad_comment,bad_comment_rate
	from
	(
		select sku_id,comment_num,has_bad_comment,bad_comment_rate,row_number() over (partition by sku_id order by dt desc) as rid
		from dev.dev_temp_msj_risk_jdata_comment where dt!='dt'
	)tmp where rid = 1
)comments
on sku_month_action.sku_id = comments.sku_id
left outer join
(
		select distinct sku_id,attr1,attr2,attr3 from dev.dev_temp_msj_risk_jdata_product
)
product
on sku_month_action.sku_id = product.sku_id
left outer join
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_1days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_1days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_1days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_1days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_1days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_1days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_1days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_1days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_1days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_1days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_1days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_1days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_1days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_1days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_1days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_1days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_1days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_1days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_1days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_1days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_1days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_1days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_1days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_1days_focus_buy_user_rate,
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as sku_1days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as sku_1days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as sku_1days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as sku_1days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as sku_1days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as sku_1days_user_avg_click_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)='${current_date}' and action_cate = 6
	group by sku_id
) sku_1days_action
on sku_month_action.sku_id = sku_1days_action.sku_id
left outer join
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_3days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_3days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_3days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_3days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_3days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_3days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_3days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_3days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_3days_click_focus_rate,
		                                                                                                     
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_3days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_3days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_3days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_3days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_3days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_3days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_3days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_3days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_3days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_3days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_3days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_3days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_3days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_3days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_3days_focus_buy_user_rate,
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as sku_3days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as sku_3days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as sku_3days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as sku_3days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as sku_3days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as sku_3days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/3 as sku_3days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/3 as sku_3days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/3 as sku_3days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/3 as sku_3days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/3 as sku_3days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/3 as sku_3days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/3 as sku_3days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/3 as sku_3days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/3 as sku_3days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/3 as sku_3days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/3 as sku_3days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/3 as sku_3days_eachday_avg_click_user_cnt
		
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',2) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by sku_id
) sku_3days_action
on sku_month_action.sku_id = sku_3days_action.sku_id
left outer join
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_5days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_5days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_5days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_5days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_5days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_5days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_5days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_5days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_5days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_5days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_5days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_5days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_5days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_5days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_5days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_5days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_5days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_5days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_5days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_5days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_5days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_5days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_5days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_5days_focus_buy_user_rate,
		
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as sku_5days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as sku_5days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as sku_5days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as sku_5days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as sku_5days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as sku_5days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/5 as sku_5days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/5 as sku_5days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/5 as sku_5days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/5 as sku_5days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/5 as sku_5days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/5 as sku_5days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/5 as sku_5days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/5 as sku_5days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/5 as sku_5days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/5 as sku_5days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/5 as sku_5days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/5 as sku_5days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',4) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by sku_id
) sku_5days_action
on sku_month_action.sku_id = sku_5days_action.sku_id
left outer join
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_7days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_7days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_7days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_7days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_7days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_7days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_7days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_7days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_7days_click_focus_rate,
		                                                                                                     
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_7days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_7days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_7days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_7days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_7days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_7days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_7days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_7days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_7days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_7days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_7days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_7days_click_focus_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_7days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_7days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_7days_focus_buy_user_rate,
		
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as sku_7days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as sku_7days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as sku_7days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as sku_7days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as sku_7days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as sku_7days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/7 as sku_7days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/7 as sku_7days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/7 as sku_7days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/7 as sku_7days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/7 as sku_7days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/7 as sku_7days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/7 as sku_7days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/7 as sku_7days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/7 as sku_7days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/7 as sku_7days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/7 as sku_7days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/7 as sku_7days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',6) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by sku_id
) sku_7days_action
on sku_month_action.sku_id = sku_7days_action.sku_id
left outer join
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_10days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_10days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_10days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_10days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_10days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_10days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_10days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_10days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_10days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_10days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_10days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_10days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_10days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_10days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_10days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_10days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_10days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_10days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_10days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_10days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_10days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_10days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_10days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_10days_focus_buy_user_rate,
		
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as sku_10days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as sku_10days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as sku_10days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as sku_10days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as sku_10days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as sku_10days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/10 as sku_10days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/10 as sku_10days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/10 as sku_10days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/10 as sku_10days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/10 as sku_10days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/10 as sku_10days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/10 as sku_10days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/10 as sku_10days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/10 as sku_10days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/10 as sku_10days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/10 as sku_10days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/10 as sku_10days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',9) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by sku_id
) sku_10days_action
on sku_month_action.sku_id = sku_10days_action.sku_id
left outer join
(
	select sku_id,
		sum(case when action_type=1 then 1 else 0 end) as sku_15days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as sku_15days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as sku_15days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as sku_15days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as sku_15days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as sku_15days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_15days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_15days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as sku_15days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as sku_15days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as sku_15days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as sku_15days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as sku_15days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as sku_15days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as sku_15days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as sku_15days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as sku_15days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as sku_15days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_15days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_15days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as sku_15days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as sku_15days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as sku_15days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as sku_15days_focus_buy_user_rate,
		
	
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as sku_15days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as sku_15days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as sku_15days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as sku_15days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as sku_15days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as sku_15days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/15 as sku_15days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/15 as sku_15days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/15 as sku_15days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/15 as sku_15days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/15 as sku_15days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/15 as sku_15days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/15 as sku_15days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/15 as sku_15days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/15 as sku_15days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/15 as sku_15days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/15 as sku_15days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/15 as sku_15days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',14) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by sku_id
) sku_15days_action
on sku_month_action.sku_id = sku_15days_action.sku_id