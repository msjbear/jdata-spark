select
		*
from
		(
				select
						a.*,
						row_number() over(partition by a.user_id order by prob
						desc) as rid
				from
						(
								select distinct user_id, sku_id, prob from
										future_predict_table
						)
						a
				left outer JOIN
						(
								select
										user_id,
										sku_id
								from
										dev.dev_temp_msj_risk_jdata_action_wide_table
								where
										substring(action_time, 1, 10)     >= date_sub('$init_date',5)
										and substring(action_time, 1, 10) <= date_sub('$init_date',1)
										and action_type = 4
								group by
										user_id,
										sku_id
						)
						b
				on
						a.user_id    = b.user_id
						and a.sku_id = b.sku_id
				where
						b.user_id    is null
						and b.sku_id is null
		)
		tmp
where
		rid = 1
order by
		prob desc limit 12000
