select distinct
            a.user_id,
            a.label  ,
            prob_1   ,
            prob_2   ,
            prob_3   ,
            prob_4   ,
            prob_5   ,
            prob_6
    from
            (
                    select distinct
                            user_id       ,
                            label         ,
                            prob       as prob_1
                    from
                            predict_eval_result_table_part1
            )
            a
    join
            (
                    select distinct
                            user_id       ,
                            prob       as prob_2
                    from
                            predict_eval_result_table_part2
            )
            b
    on
            a.user_id    = b.user_id
    join
            (
                    select distinct
                            user_id       ,
                            prob       as prob_3
                    from
                            predict_eval_result_table_part3
            )
            c
    on
            a.user_id    = c.user_id
    join
            (
                    select distinct
                            user_id       ,
                            prob       as prob_4
                    from
                            predict_eval_result_table_part4
            )
            d
    on
            a.user_id    = d.user_id
    join
            (
                    select distinct
                            user_id       ,
                            prob       as prob_5
                    from
                            predict_eval_result_table_part5
            )
            e
    on
            a.user_id    = e.user_id
	join
            (
                    select distinct
                            user_id       ,
                            prob       as prob_6
                    from
                            predict_eval_result_table_part6
            )
            f
    on
            a.user_id    = f.user_id
