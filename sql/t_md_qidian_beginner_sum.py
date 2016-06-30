----------------------------------------------
-- t_md_qidian_beginner_sum
-- [qidian > activity > beginner task overview]
-- 20160629        luohao     create
----------------------------------------------

-- drop table t_sd_qidian_dsl_activity
CREATE TABLE IF NOT EXISTS t_sd_qidian_dsl_activity (
    statis_day INT,
    userid BIGINT,
    p1 STRING,
    p2 STRING,
    p3 STRING,
    path STRING,
    activityid STRING,
    platform STRING,
    version STRING
) PARTITION BY LIST(statis_day) (
    PARTITION default
) STORED AS ORCFILE COMPRESS ;
ALTER TABLE t_sd_qidian_dsl_activity DROP PARTITION(P_MACRO_DATA_DATE) ;
ALTER TABLE t_sd_qidian_dsl_activity ADD PARTITION P_MACRO_DATA_DATE VALUES IN ( MACRO_DATA_DATE ) ;

INSERT OVERWRITE INTO TABLE u_wsd::t_sd_qidian_dsl_activity PARTITION( P_MACRO_DATA_DATE )
WITH t_md_qidian_dsl_activity_tmp AS (
    SELECT
        tdbank_imp_date as statis_day,
        regexp_extract(event_str, concat('userid', '=([^&]*)'), 1) as userid,
        regexp_extract(event_str, concat('p1', '=([^&]*)'), 1) AS p1,
        regexp_extract(event_str, concat('p2', '=([^&]*)'), 1) AS p2,
        regexp_extract(event_str, concat('p3', '=([^&]*)'), 1) AS p3,
        regexp_extract(event_str, concat('path', '=([^&]*)'), 1) AS path,
        regexp_extract(event_str, concat('activityid', '=([^&]*)'), 1) AS activityid,
        regexp_extract(event_str, concat('platform', '=([^&]*)'), 1) AS platform,
        regexp_extract(event_str, concat('version', '=([^&]*)'), 1) AS version
    FROM tl_qqbook_interface::qidian_dsl_activitylog_fdt0
    WHERE tdbank_imp_date = MACRO_DATA_DATE
) SELECT
    statis_day,
    userid,
    p1,
    p2,
    p3,
    path,
    activityid,
    platform,
    version
FROM t_md_qidian_dsl_activity_tmp ;

-- DROP TABLE t_md_qidian_beginner
CREATE TABLE IF NOT EXISTS t_md_qidian_beginner (
    statis_day INT,
    first_dt INT,
    last_dt INT,
    userid STRING,
    deposit_complete_days STRING,
    deposit_prize_days STRING,
    subscribe_complete_days STRING,
    subscribe_prize_days STRING,
    reward_complete_days STRING,
    reward_prize_days STRING,
    lottery_days STRING,
    title_receive_days STRING,
    c10_days STRING,
    c50_days STRING,
    c100_days STRING,
    v50_days STRING,
    v100_days STRING,
    unlock_days STRING    
) PARTITION BY LIST(statis_day) (
    PARTITION p_00000000 VALUES IN ( 00000000 )
) STORED AS ORCFILE COMPRESS ;

INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_beginner PARTITION (p_00000000)
WITH qidian_beginner_hz AS (
    SELECT CAST(userid AS STRING) as userid, 
           min(statis_day) as first_dt,
           max(statis_day) as last_dt,
           wm_concat(distinct case when p2 = 1 then statis_day end, ',', 'asc') as deposit_complete_days,
           wm_concat(distinct case when p2 = 4 then statis_day end, ',', 'asc') as deposit_prize_days,
           wm_concat(distinct case when p2 = 3 then statis_day end, ',', 'asc') as subscribe_complete_days,
           wm_concat(distinct case when p2 = 6 then statis_day end, ',', 'asc') as subscribe_prize_days,
           wm_concat(distinct case when p2 = 2 then statis_day end, ',', 'asc') as reward_complete_days,
           wm_concat(distinct case when p2 = 5 then statis_day end, ',', 'asc') as reward_prize_days,
           wm_concat(distinct case when p2 = 7 then statis_day end, ',', 'asc') as lottery_days,
           wm_concat(distinct case when p3 = 7 then statis_day end, ',', 'asc') as title_receive_days,
           wm_concat(distinct case when p3 = 1 then statis_day end, ',', 'asc') as c10_days,
           wm_concat(distinct case when p3 = 2 then statis_day end, ',', 'asc') as c50_days,
           wm_concat(distinct case when p3 = 3 then statis_day end, ',', 'asc') as c100_days,
           wm_concat(distinct case when p3 = 4 then statis_day end, ',', 'asc') as v50_days,
           wm_concat(distinct case when p3 = 5 then statis_day end, ',', 'asc') as v100_days,
           wm_concat(distinct case when p3 = 6 then statis_day end, ',', 'asc') as unlock_days
    FROM u_wsd::t_sd_qidian_dsl_activity
    WHERE statis_day = MACRO_DATA_DATE
    GROUP BY userid
) SELECT 
    00000000 as statis_day,
    least(coalesce(src.first_dt, tgt.first_dt), coalesce(tgt.first_dt, src.first_dt))  as first_dt,
    greatest(coalesce(src.first_dt, tgt.first_dt), coalesce(tgt.first_dt, src.first_dt)) as last_dt,
    coalesce(src.userid, tgt.userid) as userid,
    concat(
        coalesce(regexp_replace(tgt.deposit_complete_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.deposit_complete_days != '' then concat(src.deposit_complete_days, ',') else '' end)
    ) as deposit_complete_days,
    concat(
        coalesce(regexp_replace(tgt.deposit_prize_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.deposit_prize_days != '' then concat(src.deposit_prize_days, ',') else '' end)
    ) as deposit_prize_days,
    concat(
        coalesce(regexp_replace(tgt.subscribe_complete_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.subscribe_complete_days != '' then concat(src.subscribe_complete_days, ',') else '' end)
    ) as subscribe_complete_days,
    concat(
        coalesce(regexp_replace(tgt.subscribe_prize_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.subscribe_prize_days != '' then concat(src.subscribe_prize_days, ',') else '' end)
    ) as subscribe_prize_days,
    concat(
        coalesce(regexp_replace(tgt.reward_complete_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.reward_complete_days != '' then concat(src.reward_complete_days, ',') else '' end)
    ) as reward_complete_days,
    concat(
        coalesce(regexp_replace(tgt.reward_prize_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.reward_prize_days != '' then concat(src.reward_prize_days, ',') else '' end)
    ) as reward_prize_days,
    concat(
        coalesce(regexp_replace(tgt.lottery_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.lottery_days != '' then concat(src.lottery_days, ',') else '' end)
    ) as lottery_days,
    concat(
        coalesce(regexp_replace(tgt.title_receive_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.title_receive_days != '' then concat(src.title_receive_days, ',') else '' end)
    ) as title_receive_days,
    concat(
        coalesce(regexp_replace(tgt.c10_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.c10_days != '' then concat(src.c10_days, ',') else '' end)
    ) as c10_days,
    concat(
        coalesce(regexp_replace(tgt.c50_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.c50_days != '' then concat(src.c50_days, ',') else '' end)
    ) as c50_days,
    concat(
        coalesce(regexp_replace(tgt.c100_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.c100_days != '' then concat(src.c100_days, ',') else '' end)
    ) as c100_days,
    concat(
        coalesce(regexp_replace(tgt.v50_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.v50_days != '' then concat(src.v50_days, ',') else '' end)
    ) as v50_days,
    concat(
        coalesce(regexp_replace(tgt.v100_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.v100_days != '' then concat(src.v100_days, ',') else '' end)
    ) as v100_days,
    concat(
        coalesce(regexp_replace(tgt.unlock_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.unlock_days != '' then concat(src.unlock_days, ',') else '' end)
    ) as unlock_days
FROM u_wsd::t_md_qidian_beginner tgt
FULL OUTER JOIN qidian_beginner_hz src
  ON tgt.userid = src.userid ;

-- drop table t_md_qidian_page_beginner
CREATE TABLE IF NOT EXISTS t_md_qidian_page_beginner (
    statis_day INT,
    first_dt INT,
    last_dt INT,
    uid STRING,    
    pv_tss STRING,
    uv_days STRING,
    reward_click_tss STRING,
    reward_days STRING,
    subscribe_click_tss STRING,
    subscribe_days STRING,
    deposit_click_tss STRING,
    deposit_days STRING
) PARTITION BY LIST(statis_day) (
    PARTITION p_00000000 VALUES IN ( 00000000 )
) STORED AS ORCFILE COMPRESS ;

INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_page_beginner PARTITION (p_00000000)
WITH qidian_page_beginner AS (
    SELECT
        uid,
        min(statis_day) as first_dt,
        max(statis_day) as last_dt,
        wm_concat(case when event_type = 'P' and pageid = 'q_new_act' then regexp_replace(logtime, '-', '') end, ',', 'asc') as pv_tss,
        wm_concat(distinct case when event_type = 'P' and pageid = 'q_new_act' then statis_day end, ',', 'asc') as uv_days,
        wm_concat(case when event_type = 'A' and eventid in ('qd_Z01') then regexp_replace(logtime, '-', '') end, ',', 'asc') as reward_click_tss,
        wm_concat(distinct case when event_type = 'A' and eventid in ('qd_Z01') then statis_day end, ',', 'asc') as reward_days,
        wm_concat(case when event_type = 'A' and eventid in ('qd_Z02') then regexp_replace(logtime, '-', '') end, ',', 'asc') as subscribe_click_tss,
        wm_concat(distinct case when event_type = 'A' and eventid in ('qd_Z02') then statis_day end, ',', 'asc') as subscribe_days,
        wm_concat(case when event_type = 'A' and eventid in ('qd_Z03') then regexp_replace(logtime, '-', '') end, ',', 'asc') as deposit_click_tss,
        wm_concat(distinct case when event_type = 'A' and eventid in ('qd_Z03') then statis_day end, ',', 'asc') as deposit_days
    FROM u_wsd::t_od_qidian_pc_log
    WHERE statis_day = MACRO_DATA_DATE
    AND (eventid in ('qd_Z01', 'qd_Z02', 'qd_Z03') or pageid = 'q_new_act')
    AND event_type in ('A', 'P')
    GROUP BY uid
) SELECT
    00000000 as statis_day,
    least(coalesce(src.first_dt, tgt.first_dt), coalesce(tgt.first_dt, src.first_dt))  as first_dt,
    greatest(coalesce(src.first_dt, tgt.first_dt), coalesce(tgt.first_dt, src.first_dt)) as last_dt,
    coalesce(src.uid, tgt.uid) as uid,
    concat(
        coalesce(regexp_replace(tgt.pv_tss, 'MACRO_DATA_DATE[^,]*,', '') , ''),
        (case when src.pv_tss != '' then concat(src.pv_tss, ',') else '' end)
    ) as pv_tss,
    concat(
        coalesce(regexp_replace(tgt.uv_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.uv_days != '' then concat(src.uv_days, ',') else '' end)
    ) as uv_days,
    concat(
        coalesce(regexp_replace(tgt.reward_click_tss, 'MACRO_DATA_DATE[^,]*,', '') , ''),
        (case when src.reward_click_tss != '' then concat(src.reward_click_tss, ',') else '' end)
    ) as reward_click_tss,
    concat(
        coalesce(regexp_replace(tgt.reward_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.reward_days != '' then concat(src.reward_days, ',') else '' end)
    ) as reward_days,
    concat(
        coalesce(regexp_replace(tgt.subscribe_click_tss, 'MACRO_DATA_DATE[^,]*,', '') , ''),
        (case when src.subscribe_click_tss != '' then concat(src.subscribe_click_tss, ',') else '' end)
    ) as subscribe_click_tss,
    concat(
        coalesce(regexp_replace(tgt.subscribe_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.subscribe_days != '' then concat(src.subscribe_days, ',') else '' end)
    ) as subscribe_days,
    concat(
        coalesce(regexp_replace(tgt.deposit_click_tss, 'MACRO_DATA_DATE[^,]*,', '') , ''),
        (case when src.deposit_click_tss != '' then concat(src.deposit_click_tss, ',') else '' end)
    ) as deposit_click_tss,
    concat(
        coalesce(regexp_replace(tgt.deposit_days, 'MACRO_DATA_DATE,', '') , ''),
        (case when src.deposit_days != '' then concat(src.deposit_days, ',') else '' end)
    ) as deposit_days
FROM u_wsd::t_md_qidian_page_beginner tgt
FULL OUTER JOIN qidian_page_beginner src
 ON coalesce(src.uid, '[null]') = coalesce(tgt.uid, '[null]') ;

-- drop table t_md_qidian_beginner_sum
CREATE TABLE IF NOT EXISTS t_md_qidian_beginner_sum (
    statis_day INT,
    category STRING,
    cnt INT
) PARTITION BY LIST(statis_day) (
    PARTITION default
) STORED AS ORCFILE COMPRESS ;
ALTER TABLE t_md_qidian_beginner_sum DROP PARTITION(p_MACRO_DATA_DATE) ;
ALTER TABLE t_md_qidian_beginner_sum ADD PARTITION p_MACRO_DATA_DATE VALUES IN ( MACRO_DATA_DATE ) ;

INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_beginner_sum PARTITION (p_MACRO_DATA_DATE)
WITH t_md_qidian_beginner_expand AS (
    SELECT
        (case
            when category_tab.idx = 0 then 'deposit_complete_user_cnt'
            when category_tab.idx = 1 then 'deposit_prize_user_cnt'
            when category_tab.idx = 2 then 'subscribe_complete_user_cnt'
            when category_tab.idx = 3 then 'subscribe_prize_user_cnt'
            when category_tab.idx = 4 then 'reward_complete_user_cnt'
            when category_tab.idx = 5 then 'reward_prize_user_cnt'
            when category_tab.idx = 6 then 'lottery_user_cnt'
            when category_tab.idx = 7 then 'title_receive_user_cnt'
            when category_tab.idx = 8 then 'c10_user_cnt'
            when category_tab.idx = 9 then 'c50_user_cnt'
            when category_tab.idx = 10 then 'c100_user_cnt'
            when category_tab.idx = 11 then 'v50_user_cnt'
            when category_tab.idx = 12 then 'v100_user_cnt'
            when category_tab.idx = 13 then 'unlock_user_cnt'
        end) as category,
        category_days
    FROM u_wsd::t_md_qidian_beginner
    LATERAL VIEW posexplode( array(
        deposit_complete_days, deposit_prize_days, 
        subscribe_complete_days, subscribe_prize_days,
        reward_complete_days, reward_prize_days,
        lottery_days, title_receive_days,
        c10_days, c50_days, c100_days,
        v50_days, v100_days, unlock_days
    )) category_tab as idx, category_days
), t_md_qidian_beginner_task AS (
    SELECT
        category,
        count(case when regexp_instr(category_days, 'MACRO_DATA_DATE,') > 0 then 1 end) as cnt
    FROM t_md_qidian_beginner_expand
    GROUP BY category
), t_md_qidian_page_beginner_expand AS (
    SELECT
        (case
            when category_tab.idx = 0 then 'pv'
            when category_tab.idx = 1 then 'uv'
            when category_tab.idx = 2 then 'reward_click_cnt'
            when category_tab.idx = 3 then 'reward_uid_cnt'
            when category_tab.idx = 4 then 'subscribe_click_cnt'
            when category_tab.idx = 5 then 'subscribe_uid_cnt'
            when category_tab.idx = 6 then 'deposit_click_cnt'
            when category_tab.idx = 7 then 'deposit_uid_cnt'
        end) as category,
        category_days
    FROM u_wsd::t_md_qidian_page_beginner
    LATERAL VIEW posexplode( array(
        pv_tss, uv_days, 
        reward_click_tss, reward_days,
        subscribe_click_tss, subscribe_days,
        deposit_click_tss, deposit_days
    )) category_tab as idx, category_days
), t_md_qidian_beginner_page AS (
    SELECT
        category,
        sum( length(regexp_replace(category_days, '(MACRO_DATA_DATE)', '#$1')) - length(category_days) ) as cnt
    FROM t_md_qidian_page_beginner_expand
    GROUP BY category    
), t_md_qidian_beginner_sum_tmp AS (
    SELECT * FROM (
        SELECT category, cnt FROM t_md_qidian_beginner_task UNION ALL
        SELECT category, cnt FROM t_md_qidian_beginner_page
    )
)
SELECT 
    MACRO_DATA_DATE AS statis_day,
    category,
    cnt
FROM t_md_qidian_beginner_sum_tmp ;

