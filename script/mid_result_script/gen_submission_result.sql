select *
from
(
    select
    *,
    row_number() over(partition by user_id
        order by prob desc) as rid
    from
    future_predict_table
)
tmp
where rid = 1
order by prob desc limit 12000
