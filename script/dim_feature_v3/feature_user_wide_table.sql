	drop table dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix};
    create table if not exists dev.dev_temp_msj_risk_jdata_feature_user_train_v3_${suffix} stored as orc as
    select
			label.*,
			b.`(user_id)?+.+`
    from
		(
			select distinct user_id, 1 as label
			from dev.dev_temp_msj_risk_jdata_action_wide_table
			where substring(action_time,1,10)>=date_add('${current_date}',1) and substring(action_time,1,10)<=date_add('${current_date}',5) and action_cate = 6
			and action_type=4
		  union
			select l1.user_id as user_id, 0 as label
			from
			(
				select distinct user_id
				from dev.dev_temp_msj_risk_jdata_action_wide_table 
				where substring(action_time,1,10)>=date_add('${current_date}',1) and substring(action_time,1,10)<=date_add('${current_date}',5) and action_cate = 6
			)l1
			left outer join
			(
				select distinct user_id
				from dev.dev_temp_msj_risk_jdata_action_wide_table 
				where substring(action_time,1,10)>=date_add('${current_date}',1) and substring(action_time,1,10)<=date_add('${current_date}',5) and action_cate = 6
				and action_type=4
			)l2
			on l1.user_id = l2.user_id
			where l2.user_id is null
		) label
		join
            (
                    select *,
					user_month_browse_cnt   /user_month_browse_day_cnt      as user_month_day_avg_browse_cnt  ,
					user_month_add_cart_cnt	/user_month_add_cart_day_cnt	as user_month_day_avg_add_cart_cnt,		
					user_month_del_cart_cnt	/user_month_del_cart_day_cnt	as user_month_day_avg_del_cart_cnt,		
					user_month_order_cnt	/user_month_order_day_cnt		as user_month_day_avg_order_cnt	  ,
					user_month_focus_cnt	/user_month_focus_day_cnt		as user_month_day_avg_focus_cnt	  ,
					user_month_click_cnt	/user_month_click_day_cnt       as user_month_day_avg_click_cnt   ,
					user_month_browse_cnt   /user_month_browse_sku_cnt      as user_month_sku_avg_browse_cnt  ,
					user_month_add_cart_cnt	/user_month_add_cart_sku_cnt	as user_month_sku_avg_add_cart_cnt,		
					user_month_del_cart_cnt	/user_month_del_cart_sku_cnt	as user_month_sku_avg_del_cart_cnt,		
					user_month_order_cnt	/user_month_order_sku_cnt       as user_month_sku_avg_order_cnt	  ,
					user_month_focus_cnt	/user_month_focus_sku_cnt       as user_month_sku_avg_focus_cnt	  ,
					user_month_click_cnt	/user_month_click_sku_cnt       as user_month_sku_avg_click_cnt   ,
					user_month_browse_cnt   /user_month_browse_brand_cnt    as user_month_brand_avg_browse_cnt  ,
					user_month_add_cart_cnt	/user_month_add_cart_brand_cnt	as user_month_brand_avg_add_cart_cnt,		
					user_month_del_cart_cnt	/user_month_del_cart_brand_cnt	as user_month_brand_avg_del_cart_cnt,		
					user_month_order_cnt	/user_month_order_brand_cnt     as user_month_brand_avg_order_cnt	  ,
					user_month_focus_cnt	/user_month_focus_brand_cnt     as user_month_brand_avg_focus_cnt	  ,
					user_month_click_cnt	/user_month_click_brand_cnt     as user_month_brand_avg_click_cnt,
					
					user_15days_browse_cnt   /user_15days_browse_day_cnt      as user_15days_day_avg_browse_cnt  ,
					user_15days_add_cart_cnt /user_15days_add_cart_day_cnt	  as user_15days_day_avg_add_cart_cnt,		
					user_15days_del_cart_cnt /user_15days_del_cart_day_cnt	  as user_15days_day_avg_del_cart_cnt,		
					user_15days_order_cnt	 /user_15days_order_day_cnt		  as user_15days_day_avg_order_cnt	  ,
					user_15days_focus_cnt	 /user_15days_focus_day_cnt		  as user_15days_day_avg_focus_cnt	  ,
					user_15days_click_cnt	 /user_15days_click_day_cnt       as user_15days_day_avg_click_cnt   ,
					user_15days_browse_cnt   /user_15days_browse_sku_cnt      as user_15days_sku_avg_browse_cnt  ,
					user_15days_add_cart_cnt /user_15days_add_cart_sku_cnt	  as user_15days_sku_avg_add_cart_cnt,		
					user_15days_del_cart_cnt /user_15days_del_cart_sku_cnt	  as user_15days_sku_avg_del_cart_cnt,		
					user_15days_order_cnt	 /user_15days_order_sku_cnt       as user_15days_sku_avg_order_cnt	  ,
					user_15days_focus_cnt	 /user_15days_focus_sku_cnt       as user_15days_sku_avg_focus_cnt	  ,
					user_15days_click_cnt	 /user_15days_click_sku_cnt       as user_15days_sku_avg_click_cnt   ,
					user_15days_browse_cnt   /user_15days_browse_brand_cnt    as user_15days_brand_avg_browse_cnt  ,
					user_15days_add_cart_cnt /user_15days_add_cart_brand_cnt  as user_15days_brand_avg_add_cart_cnt,		
					user_15days_del_cart_cnt /user_15days_del_cart_brand_cnt  as user_15days_brand_avg_del_cart_cnt,		
					user_15days_order_cnt	 /user_15days_order_brand_cnt     as user_15days_brand_avg_order_cnt	  ,
					user_15days_focus_cnt	 /user_15days_focus_brand_cnt     as user_15days_brand_avg_focus_cnt	  ,
					user_15days_click_cnt	 /user_15days_click_brand_cnt     as user_15days_brand_avg_click_cnt,
					
					user_10days_browse_cnt   /user_10days_browse_day_cnt      as user_10days_day_avg_browse_cnt  ,
					user_10days_add_cart_cnt /user_10days_add_cart_day_cnt	  as user_10days_day_avg_add_cart_cnt,		
					user_10days_del_cart_cnt /user_10days_del_cart_day_cnt	  as user_10days_day_avg_del_cart_cnt,		
					user_10days_order_cnt	 /user_10days_order_day_cnt		  as user_10days_day_avg_order_cnt	  ,
					user_10days_focus_cnt	 /user_10days_focus_day_cnt		  as user_10days_day_avg_focus_cnt	  ,
					user_10days_click_cnt	 /user_10days_click_day_cnt       as user_10days_day_avg_click_cnt   ,
					user_10days_browse_cnt   /user_10days_browse_sku_cnt      as user_10days_sku_avg_browse_cnt  ,
					user_10days_add_cart_cnt /user_10days_add_cart_sku_cnt	  as user_10days_sku_avg_add_cart_cnt,		
					user_10days_del_cart_cnt /user_10days_del_cart_sku_cnt	  as user_10days_sku_avg_del_cart_cnt,		
					user_10days_order_cnt	 /user_10days_order_sku_cnt       as user_10days_sku_avg_order_cnt	  ,
					user_10days_focus_cnt	 /user_10days_focus_sku_cnt       as user_10days_sku_avg_focus_cnt	  ,
					user_10days_click_cnt	 /user_10days_click_sku_cnt       as user_10days_sku_avg_click_cnt   ,
					user_10days_browse_cnt   /user_10days_browse_brand_cnt    as user_10days_brand_avg_browse_cnt  ,
					user_10days_add_cart_cnt /user_10days_add_cart_brand_cnt  as user_10days_brand_avg_add_cart_cnt,		
					user_10days_del_cart_cnt /user_10days_del_cart_brand_cnt  as user_10days_brand_avg_del_cart_cnt,		
					user_10days_order_cnt	 /user_10days_order_brand_cnt     as user_10days_brand_avg_order_cnt	  ,
					user_10days_focus_cnt	 /user_10days_focus_brand_cnt     as user_10days_brand_avg_focus_cnt	  ,
					user_10days_click_cnt	 /user_10days_click_brand_cnt     as user_10days_brand_avg_click_cnt,
					
					user_7days_browse_cnt   /user_7days_browse_day_cnt      as user_7days_day_avg_browse_cnt  ,
					user_7days_add_cart_cnt	/user_7days_add_cart_day_cnt	as user_7days_day_avg_add_cart_cnt,		
					user_7days_del_cart_cnt	/user_7days_del_cart_day_cnt	as user_7days_day_avg_del_cart_cnt,		
					user_7days_order_cnt	/user_7days_order_day_cnt		as user_7days_day_avg_order_cnt	  ,
					user_7days_focus_cnt	/user_7days_focus_day_cnt		as user_7days_day_avg_focus_cnt	  ,
					user_7days_click_cnt	/user_7days_click_day_cnt       as user_7days_day_avg_click_cnt   ,
					user_7days_browse_cnt   /user_7days_browse_sku_cnt      as user_7days_sku_avg_browse_cnt  ,
					user_7days_add_cart_cnt	/user_7days_add_cart_sku_cnt	as user_7days_sku_avg_add_cart_cnt,		
					user_7days_del_cart_cnt	/user_7days_del_cart_sku_cnt	as user_7days_sku_avg_del_cart_cnt,		
					user_7days_order_cnt	/user_7days_order_sku_cnt       as user_7days_sku_avg_order_cnt	  ,
					user_7days_focus_cnt	/user_7days_focus_sku_cnt       as user_7days_sku_avg_focus_cnt	  ,
					user_7days_click_cnt	/user_7days_click_sku_cnt       as user_7days_sku_avg_click_cnt   ,
					user_7days_browse_cnt   /user_7days_browse_brand_cnt    as user_7days_brand_avg_browse_cnt  ,
					user_7days_add_cart_cnt	/user_7days_add_cart_brand_cnt	as user_7days_brand_avg_add_cart_cnt,		
					user_7days_del_cart_cnt	/user_7days_del_cart_brand_cnt	as user_7days_brand_avg_del_cart_cnt,		
					user_7days_order_cnt	/user_7days_order_brand_cnt     as user_7days_brand_avg_order_cnt	  ,
					user_7days_focus_cnt	/user_7days_focus_brand_cnt     as user_7days_brand_avg_focus_cnt	  ,
					user_7days_click_cnt	/user_7days_click_brand_cnt     as user_7days_brand_avg_click_cnt,
					
					
					user_5days_browse_cnt   /user_5days_browse_day_cnt      as user_5days_day_avg_browse_cnt  ,
					user_5days_add_cart_cnt	/user_5days_add_cart_day_cnt	as user_5days_day_avg_add_cart_cnt,		
					user_5days_del_cart_cnt	/user_5days_del_cart_day_cnt	as user_5days_day_avg_del_cart_cnt,		
					user_5days_order_cnt	/user_5days_order_day_cnt		as user_5days_day_avg_order_cnt	  ,
					user_5days_focus_cnt	/user_5days_focus_day_cnt		as user_5days_day_avg_focus_cnt	  ,
					user_5days_click_cnt	/user_5days_click_day_cnt       as user_5days_day_avg_click_cnt   ,
					user_5days_browse_cnt   /user_5days_browse_sku_cnt      as user_5days_sku_avg_browse_cnt  ,
					user_5days_add_cart_cnt	/user_5days_add_cart_sku_cnt	as user_5days_sku_avg_add_cart_cnt,		
					user_5days_del_cart_cnt	/user_5days_del_cart_sku_cnt	as user_5days_sku_avg_del_cart_cnt,		
					user_5days_order_cnt	/user_5days_order_sku_cnt       as user_5days_sku_avg_order_cnt	  ,
					user_5days_focus_cnt	/user_5days_focus_sku_cnt       as user_5days_sku_avg_focus_cnt	  ,
					user_5days_click_cnt	/user_5days_click_sku_cnt       as user_5days_sku_avg_click_cnt   ,
					user_5days_browse_cnt   /user_5days_browse_brand_cnt    as user_5days_brand_avg_browse_cnt  ,
					user_5days_add_cart_cnt	/user_5days_add_cart_brand_cnt	as user_5days_brand_avg_add_cart_cnt,		
					user_5days_del_cart_cnt	/user_5days_del_cart_brand_cnt	as user_5days_brand_avg_del_cart_cnt,		
					user_5days_order_cnt	/user_5days_order_brand_cnt     as user_5days_brand_avg_order_cnt	  ,
					user_5days_focus_cnt	/user_5days_focus_brand_cnt     as user_5days_brand_avg_focus_cnt	  ,
					user_5days_click_cnt	/user_5days_click_brand_cnt     as user_5days_brand_avg_click_cnt,
					
					user_3days_browse_cnt   /user_3days_browse_day_cnt      as user_3days_day_avg_browse_cnt  ,
					user_3days_add_cart_cnt	/user_3days_add_cart_day_cnt	as user_3days_day_avg_add_cart_cnt,		
					user_3days_del_cart_cnt	/user_3days_del_cart_day_cnt	as user_3days_day_avg_del_cart_cnt,		
					user_3days_order_cnt	/user_3days_order_day_cnt		as user_3days_day_avg_order_cnt	  ,
					user_3days_focus_cnt	/user_3days_focus_day_cnt		as user_3days_day_avg_focus_cnt	  ,
					user_3days_click_cnt	/user_3days_click_day_cnt       as user_3days_day_avg_click_cnt   ,
					user_3days_browse_cnt   /user_3days_browse_sku_cnt      as user_3days_sku_avg_browse_cnt  ,
					user_3days_add_cart_cnt	/user_3days_add_cart_sku_cnt	as user_3days_sku_avg_add_cart_cnt,		
					user_3days_del_cart_cnt	/user_3days_del_cart_sku_cnt	as user_3days_sku_avg_del_cart_cnt,		
					user_3days_order_cnt	/user_3days_order_sku_cnt       as user_3days_sku_avg_order_cnt	  ,
					user_3days_focus_cnt	/user_3days_focus_sku_cnt       as user_3days_sku_avg_focus_cnt	  ,
					user_3days_click_cnt	/user_3days_click_sku_cnt       as user_3days_sku_avg_click_cnt   ,
					user_3days_browse_cnt   /user_3days_browse_brand_cnt    as user_3days_brand_avg_browse_cnt  ,
					user_3days_add_cart_cnt	/user_3days_add_cart_brand_cnt	as user_3days_brand_avg_add_cart_cnt,		
					user_3days_del_cart_cnt	/user_3days_del_cart_brand_cnt	as user_3days_brand_avg_del_cart_cnt,		
					user_3days_order_cnt	/user_3days_order_brand_cnt     as user_3days_brand_avg_order_cnt	  ,
					user_3days_focus_cnt	/user_3days_focus_brand_cnt     as user_3days_brand_avg_focus_cnt	  ,
					user_3days_click_cnt	/user_3days_click_brand_cnt     as user_3days_brand_avg_click_cnt

					from dev.dev_temp_msj_risk_jdata_feature_user_dim_v3_${suffix}
            )
            b
    on
            label.user_id = b.user_id
