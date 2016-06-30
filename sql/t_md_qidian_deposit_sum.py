----------------------------------------------
-- t_md_qidian_deposit_sum
-- [qidian > revenue > deposit overview]
-- 20160621        luohao     create
----------------------------------------------

-- drop table t_td_qidian_deposit_amt_hz
CREATE TABLE IF NOT EXISTS t_td_qidian_deposit_amt_hz(
    statis_day INT,
    platform STRING,
    amt_all DOUBLE,
    amt_price DOUBLE,
    amt_gift_price DOUBLE,
    amt_free DOUBLE,
    amt_red_envelope DOUBLE
) PARTITION BY LIST(statis_day) (
    PARTITION default
) STORED AS ORCFILE COMPRESS ;
ALTER TABLE t_td_qidian_deposit_amt_hz DROP PARTITION(P_MACRO_DATA_DATE) ;
ALTER TABLE t_td_qidian_deposit_amt_hz ADD PARTITION P_MACRO_DATA_DATE VALUES IN ( MACRO_DATA_DATE ) ;

INSERT OVERWRITE INTO TABLE u_wsd::t_td_qidian_deposit_amt_hz PARTITION(P_MACRO_DATA_DATE)
WITH qidian_deposit_amt AS (
    SELECT * FROM (
        SELECT statis_day,
               'userdeposit' as data_source,
               (case
                 when appid = '10' then 'pc'
                 when appid = '13' then 'm'
                 when appid = '12' then 'android'
                 when appid = '33' then 'ios'
                 else 'other'
               end) as platform,
               currency,
               deposit_source,
               point / 100 as amt
        FROM wsd::t_sd_qidian_userdeposit
        WHERE statis_day = MACRO_DATA_DATE
        UNION ALL
        SELECT statis_day,
               'iap' as data_source,
               'ios' as platform,
               null as currency,
               null as deposit_source,
               rmb_amount as amt
        FROM wsd::t_sd_qidian_iapdeposit
        WHERE statis_day = MACRO_DATA_DATE
    )
), qidian_deposit_amt_hz AS (
    SELECT
        statis_day,
        (case when GROUPING(platform) = 1 then 'all' else platform end) as platform,
        sum(amt) as amt_all,
        sum(case when currency in ('13', '35') or data_source = 'iap' then amt end) as amt_price,
        sum(case when currency in ('1', '10', '102', '11', '18', '20', '30', '33', '34', '36', '38') then amt end) as amt_gift_price,
        sum(case when currency not in ('13', '35', '1', '10', '102', '11', '18', '20', '30', '33', '34', '36', '38') then amt end) as amt_free,
        sum(case when currency in ('38') and deposit_source in ('159', '169')  then amt end) as amt_red_envelope
    FROM qidian_deposit_amt
    group by statis_day, rollup(platform)
) SELECT statis_day, platform, amt_all, amt_price, amt_gift_price, amt_free, amt_red_envelope  FROM qidian_deposit_amt_hz ;

-- DROP TABLE t_md_qidian_deposit_amt
CREATE TABLE IF NOT EXISTS t_md_qidian_deposit_amt (
    statis_day INT,
    platform STRING,
    category STRING,
    amt DOUBLE
) PARTITION BY LIST(statis_day) (
    PARTITION default
) STORED AS ORCFILE COMPRESS ;
ALTER TABLE t_md_qidian_deposit_amt DROP PARTITION(P_MACRO_DATA_DATE) ;
ALTER TABLE t_md_qidian_deposit_amt ADD PARTITION P_MACRO_DATA_DATE VALUES IN ( MACRO_DATA_DATE ) ;

-- set hive.optimize.ppd = false
INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_deposit_amt PARTITION(P_MACRO_DATA_DATE)
WITH t_md_qidian_userdeposit_amt_tmp AS (
    SELECT
        statis_day ,
        platform ,
        (case
            when category_tab.idx = 0 then 'total'
            when category_tab.idx = 1 then 'price'
            when category_tab.idx = 2 then 'gift_price'
            when category_tab.idx = 3 then 'free'
            when category_tab.idx = 4 then 'red_envelope'
        end) as category,
        amt
    FROM u_wsd::t_td_qidian_deposit_amt_hz
    LATERAL VIEW posexplode(array(amt_all, amt_price, amt_gift_price, amt_free, amt_red_envelope)) category_tab as idx, amt
    WHERE statis_day >= MACRO_DATA_DATE
) SELECT statis_day,
       platform,
       category,
       amt
FROM t_md_qidian_userdeposit_amt_tmp ;

-- drop table t_md_qidian_deposit_user
CREATE TABLE IF NOT EXISTS t_md_qidian_deposit_user  (
    statis_day INT,
    first_dt INT,
    last_dt INT,
    username STRING,
    platform STRING,
    all_days STRING,
    price_days STRING,
    gift_price_days  STRING,
    free_days STRING,
    red_envelope_days STRING,
    all_months STRING,
    price_months STRING,
    gift_price_months STRING,
    free_months STRING,
    red_envelope_months STRING
)  PARTITION BY LIST(statis_day) (
    PARTITION p_00000000 VALUES IN ( 00000000 )
) STORED AS ORCFILE COMPRESS ;

INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_deposit_user PARTITION (p_00000000)
WITH qidian_deposit_user AS (
    SELECT * FROM (
        SELECT statis_day,
               'userdeposit' as data_source,
               (case
                 when appid = '10' then 'pc'
                 when appid = '13' then 'm'
                 when appid = '12' then 'android'
                 when appid = '33' then 'ios'
                 else 'other'
               end) as platform,
               currency,
               deposit_source,
               username
        FROM wsd::t_sd_qidian_userdeposit
        WHERE statis_day = MACRO_DATA_DATE
        UNION ALL
        SELECT statis_day,
               'iap' as data_source,
               'ios' as platform,
               null as currency,
               null as deposit_source,
               logid AS username
        FROM wsd::t_sd_qidian_iapdeposit
        WHERE statis_day = MACRO_DATA_DATE
    )
), qidian_deposit_user_hz AS (
    SELECT
        username,
        (case when GROUPING(platform) = 1 then 'all' else platform end) as platform,
        min(statis_day) as first_dt,
        max(statis_day) as last_dt,
        wm_concat(distinct statis_day, ',', 'asc') as all_days,
        wm_concat(distinct case when currency in ('13', '35') or data_source = 'iap' then  statis_day end, ',', 'asc') as price_days,
        wm_concat(distinct case when currency in ('1', '10', '102', '11', '18', '20', '30', '33', '34', '36', '38') then statis_day end, ',', 'asc') as gift_price_days,
        wm_concat(distinct case when currency not in ('13', '35', '1', '10', '102', '11', '18', '20', '30', '33', '34', '36', '38') then statis_day end, ',', 'asc') as free_days,
        wm_concat(distinct case when currency in ('38') and deposit_source in ('159', '169') then statis_day end, ',', 'asc') as red_envelope_days,
        wm_concat(distinct substr(statis_day, 0, 6), ',', 'asc') as all_months,
        wm_concat(distinct case when currency in ('13', '35') or data_source = 'iap' then substr(statis_day, 1, 6) end, ',', 'asc') as price_months,
        wm_concat(distinct case when currency in ('1', '10', '102', '11', '18', '20', '30', '33', '34', '36', '38') then substr(statis_day, 1, 6) end, ',', 'asc') as gift_price_months,
        wm_concat(distinct case when currency not in ('13', '35', '1', '10', '102', '11', '18', '20', '30', '33', '34', '36', '38') then substr(statis_day, 1, 6) end, ',', 'asc') as free_months,
        wm_concat(distinct case when currency in ('38') and deposit_source in ('159', '169') then substr(statis_day, 1, 6) end, ',', 'asc') as red_envelope_months
    FROM qidian_deposit_user
    group by username, rollup(platform)
) SELECT
       00000000 as statis_day,
       least(coalesce(src.first_dt, tgt.first_dt), coalesce(tgt.first_dt, src.first_dt))  as first_dt,
       greatest(coalesce(src.first_dt, tgt.first_dt), coalesce(tgt.first_dt, src.first_dt)) as last_dt,
       coalesce(src.username, tgt.username) as username,
       coalesce(src.platform, tgt.platform) as platform,
       concat(
           coalesce(regexp_replace(tgt.all_days, 'MACRO_DATA_DATE,', '') , ''),
           (case when src.all_days != '' then concat(src.all_days, ',') else '' end)
       ) as all_days,
       concat(
           coalesce(regexp_replace(tgt.price_days, 'MACRO_DATA_DATE,', '') , ''),
           (case when src.price_days != '' then concat(src.price_days, ',') else '' end)
       ) as price_days,
       concat(
           coalesce(regexp_replace(tgt.gift_price_days, 'MACRO_DATA_DATE,', '') , ''),
           (case when src.gift_price_days != '' then concat(src.gift_price_days, ',') else '' end)
       ) as gift_price_days,
       concat(
           coalesce(regexp_replace(tgt.free_days, 'MACRO_DATA_DATE,', '') , ''),
           (case when src.free_days != '' then concat(src.free_days, ',') else '' end)
       ) as free_days,
       concat(
           coalesce(regexp_replace(tgt.red_envelope_days, 'MACRO_DATA_DATE,', '') , ''),
           (case when src.red_envelope_days != '' then concat(src.red_envelope_days, ',') else '' end)
       ) as red_envelope_days,
       concat(
           coalesce(
             case 
               when regexp_instr( regexp_replace(tgt.all_days, 'MACRO_DATA_DATE,', ''), substr('MACRO_DATA_DATE', 1, 6)) > 0
                and not regexp_instr( src.all_months, substr('MACRO_DATA_DATE', 1, 6) ) > 0
               then tgt.all_months 
               else regexp_replace(tgt.all_months, concat(substr('MACRO_DATA_DATE', 1, 6), ','), '')
             end,
             ''
           ),
           (case when src.all_months != '' then concat(src.all_months, ',') else '' end)
       ) as all_months,
       concat(
           coalesce(
             case 
               when regexp_instr( regexp_replace(tgt.price_days, 'MACRO_DATA_DATE,', ''), substr('MACRO_DATA_DATE', 1, 6)) > 0
                and not regexp_instr( src.price_months, substr('MACRO_DATA_DATE', 1, 6) ) > 0
               then tgt.price_months
               else regexp_replace(tgt.price_months, concat(substr('MACRO_DATA_DATE', 1, 6), ','), '')
             end,
             ''
           ),
           (case when src.price_months != '' then concat(src.price_months, ',') else '' end)
       ) as price_months,
       concat(
           coalesce(
             case 
               when regexp_instr( regexp_replace(tgt.gift_price_days, 'MACRO_DATA_DATE,', ''), substr('MACRO_DATA_DATE', 1, 6)) > 0
                and not regexp_instr( src.gift_price_months, substr('MACRO_DATA_DATE', 1, 6) ) > 0
               then tgt.gift_price_months
               else regexp_replace(tgt.gift_price_months, concat(substr('MACRO_DATA_DATE', 1, 6), ','), '') 
             end,
             ''
           ),
           (case when src.gift_price_months != '' then concat(src.gift_price_months, ',') else '' end)
       ) as gift_price_months,
       concat(
           coalesce(
             case 
               when regexp_instr( regexp_replace(tgt.free_days, 'MACRO_DATA_DATE,', ''), substr('MACRO_DATA_DATE', 1, 6) ) > 0
                and not regexp_instr( src.free_months, substr('MACRO_DATA_DATE', 1, 6) ) > 0
               then tgt.free_months
               else regexp_replace(tgt.free_months, concat(substr('MACRO_DATA_DATE', 1, 6), ','), '')
             end,
             ''
           ),
           (case when src.free_months != '' then concat(src.free_months, ',') else '' end)
       ) as free_months,
       concat(
           coalesce(
             case 
               when regexp_instr( regexp_replace(tgt.red_envelope_days, 'MACRO_DATA_DATE,', ''), substr('MACRO_DATA_DATE', 1, 6)) > 0
               and not regexp_instr( src.red_envelope_months, substr('MACRO_DATA_DATE', 1, 6) ) > 0
               then tgt.red_envelope_months
               else regexp_replace(tgt.red_envelope_months, concat(substr('MACRO_DATA_DATE', 1, 6), ','), '')
             end,
             ''
           ),
           (case when src.red_envelope_months != '' then concat(src.red_envelope_months, ',') else '' end)
       ) as red_envelope_months
  FROM u_wsd::t_md_qidian_deposit_user tgt
  FULL OUTER JOIN qidian_deposit_user_hz src
    ON tgt.username = src.username
   AND tgt.platform = src.platform ;

-- drop table t_md_qidian_deposit_cnt
CREATE TABLE IF NOT EXISTS t_md_qidian_deposit_cnt  (    
    statis_day INT,
    platform STRING,
    category STRING,
    cnt INT
) PARTITION BY LIST(statis_day) (
    PARTITION default
) STORED AS ORCFILE COMPRESS ;
ALTER TABLE t_md_qidian_deposit_cnt DROP PARTITION(P_MACRO_DATA_DATE) ;
ALTER TABLE t_md_qidian_deposit_cnt ADD PARTITION P_MACRO_DATA_DATE VALUES IN ( MACRO_DATA_DATE ) ;

INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_deposit_cnt PARTITION(P_MACRO_DATA_DATE)
WITH t_md_qidian_deposit_cnt_tmp AS (
    SELECT platform,
        (case
            when category_tab.idx= 0 then 'total'
            when category_tab.idx = 1 then 'price'
            when category_tab.idx = 2 then 'gift_price'
            when category_tab.idx = 3 then 'free'
            when category_tab.idx = 4 then 'red_envelope'
        end) as category,
        deposit_days
    FROM u_wsd::t_md_qidian_deposit_user
    LATERAL VIEW posexplode( array(all_days, price_days, gift_price_days, free_days, red_envelope_days)) category_tab as idx, deposit_days
)
SELECT MACRO_DATA_DATE,
       platform,
       category,
       count(case when regexp_instr(deposit_days, 'MACRO_DATA_DATE,') > 0 then 1 end)
FROM t_md_qidian_deposit_cnt_tmp
GROUP BY platform, category ;

-- drop table t_md_qidian_deposit_sum
CREATE TABLE IF NOT EXISTS t_md_qidian_deposit_sum (
    statis_day INT,
    platform STRING,
    category STRING,
    amt DOUBLE,
    cnt INT
) PARTITION BY LIST(statis_day) (
    PARTITION default
) STORED AS ORCFILE COMPRESS ;
ALTER TABLE t_md_qidian_deposit_sum DROP PARTITION(P_MACRO_DATA_DATE) ;
ALTER TABLE t_md_qidian_deposit_sum ADD PARTITION P_MACRO_DATA_DATE VALUES IN ( MACRO_DATA_DATE ) ;

INSERT OVERWRITE INTO TABLE u_wsd::t_md_qidian_deposit_sum PARTITION(P_MACRO_DATA_DATE)
with t_md_qidian_deposit_sum_tmp AS (
    SELECT coalesce(amt_tab.statis_day, cnt_tab.statis_day) as statis_day,
           coalesce(amt_tab.platform, cnt_tab.platform) as platform,
           coalesce(amt_tab.category, cnt_tab.category) as category,
           amt_tab.amt,
           cnt_tab.cnt
    FROM u_wsd::t_md_qidian_deposit_amt amt_tab
    FULL OUTER JOIN u_wsd::t_md_qidian_deposit_cnt cnt_tab
      ON amt_tab.statis_day = cnt_tab.statis_day
     AND amt_tab.platform = cnt_tab.platform
     AND amt_tab.category = cnt_tab.category
     AND cnt_tab.statis_day = MACRO_DATA_DATE
    WHERE amt_tab.statis_day = MACRO_DATA_DATE
) SELECT statis_day,
         platform,
         category,
         amt,
         cnt
FROM t_md_qidian_deposit_sum_tmp ;

