drop table dev.dev_temp_msj_risk_jdata_feature_brand_dim_v3_${suffix};
create table if not exists dev.dev_temp_msj_risk_jdata_feature_brand_dim_v3_${suffix} stored as orc as select
	brand_month_action.*,
	brand_1days_action.`(action_brand)?+.+`,
	brand_3days_action.`(action_brand)?+.+`,
	brand_5days_action.`(action_brand)?+.+`,
	brand_7days_action.`(action_brand)?+.+`,
	brand_10days_action.`(action_brand)?+.+`,
	brand_15days_action.`(action_brand)?+.+`
from
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_month_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_month_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_month_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_month_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_month_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_month_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_month_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_month_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_month_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_month_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_month_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_month_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_month_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_month_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_month_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_month_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_month_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_month_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_month_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_month_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_month_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_month_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_month_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_month_focus_buy_user_rate,
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_month_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_month_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end) as brand_month_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end) as brand_month_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_month_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_month_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/30 as brand_month_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/30 as brand_month_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/30 as brand_month_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/30 as brand_month_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/30 as brand_month_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/30 as brand_month_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/30 as brand_month_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/30 as brand_month_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/30 as brand_month_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/30 as brand_month_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/30 as brand_month_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/30 as brand_month_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',30) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by action_brand
) brand_month_action
left outer join
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_1days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_1days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_1days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_1days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_1days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_1days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_1days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_1days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_1days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_1days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_1days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_1days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_1days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_1days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_1days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_1days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_1days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_1days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_1days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_1days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_1days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_1days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_1days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_1days_focus_buy_user_rate,
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as brand_1days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as brand_1days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as brand_1days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as brand_1days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as brand_1days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as brand_1days_user_avg_click_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)='${current_date}' and action_cate = 6
	group by action_brand
) brand_1days_action
on brand_month_action.action_brand = brand_1days_action.action_brand
left outer join
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_3days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_3days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_3days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_3days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_3days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_3days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_3days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_3days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_3days_click_focus_rate,
		                                                                                                     
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_3days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_3days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_3days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_3days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_3days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_3days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_3days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_3days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_3days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_3days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_3days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_3days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_3days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_3days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_3days_focus_buy_user_rate,
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as brand_3days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as brand_3days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as brand_3days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as brand_3days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as brand_3days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as brand_3days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/3 as brand_3days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/3 as brand_3days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/3 as brand_3days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/3 as brand_3days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/3 as brand_3days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/3 as brand_3days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/3 as brand_3days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/3 as brand_3days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/3 as brand_3days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/3 as brand_3days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/3 as brand_3days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/3 as brand_3days_eachday_avg_click_user_cnt
		
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',2) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by action_brand
) brand_3days_action
on brand_month_action.action_brand = brand_3days_action.action_brand
left outer join
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_5days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_5days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_5days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_5days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_5days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_5days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_5days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_5days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_5days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_5days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_5days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_5days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_5days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_5days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_5days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_5days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_5days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_5days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_5days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_5days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_5days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_5days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_5days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_5days_focus_buy_user_rate,
		
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as brand_5days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as brand_5days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as brand_5days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as brand_5days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as brand_5days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as brand_5days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/5 as brand_5days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/5 as brand_5days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/5 as brand_5days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/5 as brand_5days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/5 as brand_5days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/5 as brand_5days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/5 as brand_5days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/5 as brand_5days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/5 as brand_5days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/5 as brand_5days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/5 as brand_5days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/5 as brand_5days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',4) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by action_brand
) brand_5days_action
on brand_month_action.action_brand = brand_5days_action.action_brand
left outer join
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_7days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_7days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_7days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_7days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_7days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_7days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_7days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_7days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_7days_click_focus_rate,
		                                                                                                     
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_7days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_7days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_7days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_7days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_7days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_7days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_7days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_7days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_7days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_7days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_7days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_7days_click_focus_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_7days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_7days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_7days_focus_buy_user_rate,
		
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as brand_7days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as brand_7days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as brand_7days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as brand_7days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as brand_7days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as brand_7days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/7 as brand_7days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/7 as brand_7days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/7 as brand_7days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/7 as brand_7days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/7 as brand_7days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/7 as brand_7days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/7 as brand_7days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/7 as brand_7days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/7 as brand_7days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/7 as brand_7days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/7 as brand_7days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/7 as brand_7days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',6) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by action_brand
) brand_7days_action
on brand_month_action.action_brand = brand_7days_action.action_brand
left outer join
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_10days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_10days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_10days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_10days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_10days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_10days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_10days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_10days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_10days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_10days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_10days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_10days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_10days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_10days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_10days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_10days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_10days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_10days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_10days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_10days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_10days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_10days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_10days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_10days_focus_buy_user_rate,
		
		
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as brand_10days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as brand_10days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as brand_10days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as brand_10days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as brand_10days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as brand_10days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/10 as brand_10days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/10 as brand_10days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/10 as brand_10days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/10 as brand_10days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/10 as brand_10days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/10 as brand_10days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/10 as brand_10days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/10 as brand_10days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/10 as brand_10days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/10 as brand_10days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/10 as brand_10days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/10 as brand_10days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',9) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by action_brand
) brand_10days_action
on brand_month_action.action_brand = brand_10days_action.action_brand
left outer join
(
	select action_brand,
		sum(case when action_type=1 then 1 else 0 end) as brand_15days_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end) as brand_15days_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end) as brand_15days_del_cart_cnt, 
		sum(case when action_type=4 then 1 else 0 end) as brand_15days_order_cnt, 
		sum(case when action_type=5 then 1 else 0 end) as brand_15days_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end) as brand_15days_click_cnt,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_15days_click_buy_rate,
		sum(case when action_type=2 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_15days_click_cart_rate,
		sum(case when action_type=5 then 1 else 0 end)/sum(case when action_type=6 then 1 else 0 end) as brand_15days_click_focus_rate,
		
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=1 then 1 else 0 end) as brand_15days_browse_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=2 then 1 else 0 end) as brand_15days_cart_buy_rate,
		sum(case when action_type=4 then 1 else 0 end)/sum(case when action_type=5 then 1 else 0 end) as brand_15days_focus_buy_rate,
		
		count(distinct case when action_type=1 then user_id else NULL end) as brand_15days_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end) as brand_15days_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end) as brand_15days_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end) as brand_15days_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end) as brand_15days_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end) as brand_15days_click_user_cnt,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_15days_click_buy_user_rate,
		count(distinct case when action_type=2 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_15days_click_cart_user_rate,
		count(distinct case when action_type=5 then user_id else NULL end)/count(distinct case when action_type=6 then user_id else NULL end) as brand_15days_click_focus_user_rate,
		
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=1 then user_id else NULL end) as brand_15days_browse_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=2 then user_id else NULL end) as brand_15days_cart_buy_user_rate,
		count(distinct case when action_type=4 then user_id else NULL end)/count(distinct case when action_type=5 then user_id else NULL end) as brand_15days_focus_buy_user_rate,
		
	
		sum(case when action_type=1 then 1 else 0 end)/count(distinct case when action_type=1 then user_id else NULL end)  as brand_15days_user_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/count(distinct case when action_type=2 then user_id else NULL end)  as brand_15days_user_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/count(distinct case when action_type=3 then user_id else NULL end)  as brand_15days_user_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/count(distinct case when action_type=4 then user_id else NULL end)  as brand_15days_user_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/count(distinct case when action_type=5 then user_id else NULL end)  as brand_15days_user_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/count(distinct case when action_type=6 then user_id else NULL end)  as brand_15days_user_avg_click_cnt,
		
		sum(case when action_type=1 then 1 else 0 end)/15 as brand_15days_eachday_avg_browse_cnt,
		sum(case when action_type=2 then 1 else 0 end)/15 as brand_15days_eachday_avg_add_cart_cnt,
		sum(case when action_type=3 then 1 else 0 end)/15 as brand_15days_eachday_avg_del_cart_cnt,
		sum(case when action_type=4 then 1 else 0 end)/15 as brand_15days_eachday_avg_order_cnt,
		sum(case when action_type=5 then 1 else 0 end)/15 as brand_15days_eachday_avg_focus_cnt,
		sum(case when action_type=6 then 1 else 0 end)/15 as brand_15days_eachday_avg_click_cnt,
		
		count(distinct case when action_type=1 then user_id else NULL end)/15 as brand_15days_eachday_avg_browse_user_cnt,
		count(distinct case when action_type=2 then user_id else NULL end)/15 as brand_15days_eachday_avg_add_cart_user_cnt,
		count(distinct case when action_type=3 then user_id else NULL end)/15 as brand_15days_eachday_avg_del_cart_user_cnt,
		count(distinct case when action_type=4 then user_id else NULL end)/15 as brand_15days_eachday_avg_order_user_cnt,
		count(distinct case when action_type=5 then user_id else NULL end)/15 as brand_15days_eachday_avg_focus_user_cnt,
		count(distinct case when action_type=6 then user_id else NULL end)/15 as brand_15days_eachday_avg_click_user_cnt
		
	from dev.dev_temp_msj_risk_jdata_action_wide_table 
	where substring(action_time,1,10)>=date_sub('${current_date}',14) and substring(action_time,1,10)<='${current_date}' and action_cate = 6
	group by action_brand
) brand_15days_action
on brand_month_action.action_brand = brand_15days_action.action_brand