
--创建本地维表
CREATE TABLE IF NOT EXISTS hdy_local_air_time_info
(
    dep_prov_id string,
    dep_city_id string,
    dep_airport string,
    dep_time    string,
    arr_time    string,
    arr_prov_id string,
    arr_city_id string,
    arr_airport string
) PARTITIONED BY (month_id) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

LOAD DATA INPATH 'air_time.csv' INTO TABLE hdy_local_air_time_info PARTITION (month_id = '${month_id}');

--创建中间表
CREATE TABLE IF NOT EXISTS hdy_temp_available_gprs_data
(
    phone_no        string,
    city_id         string,
    start_datetime  string,
    visit_area_code string,
    roam_type       string
) PARTITIONED BY (dt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

CREATE TABLE IF NOT EXISTS hdy_temp_roam_prov
(
    phone_no    string,
    city_id     string,
    dep_time    string,
    dep_prov_id string,
    arr_time    string,
    arr_prov_id string
) PARTITIONED BY (month_id) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

CREATE TABLE IF NOT EXISTS hdy_temp_air_in_analysis
(
    phone_no    string,
    dep_time    string,
    dep_city_id string,
    dep_airport string,
    arr_time    string,
    arr_city_id string,
    arr_airport string,
    stay_place  string
) PARTITIONED BY (dt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

CREATE TABLE IF NOT EXISTS hdy_temp_air_out_analysis
(
    phone_no    string,
    dep_time    string,
    dep_city_id string,
    dep_airport string,
    arr_time    string,
    arr_city_id string,
    arr_airport string,
    stay_place  string
) PARTITIONED BY (dt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

CREATE TABLE IF NOT EXISTS hdy_temp_in_out_combined
(
    phone_no       string,
    live_city_id   string,
    belong_city_id string,
    dep_time       string,
    dep_city_id    string,
    dep_airport    string,
    arr_time       string,
    arr_city_id    string,
    arr_airport    string,
    in_out_label   string
) PARTITIONED BY (month_id) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

CREATE TABLE IF NOT EXISTS output_detailed
(
    phone_no       string,
    sex            string,
    age            string,
    bill_fee       string,
    live_city_id   string,
    belong_city_id string,
    dep_time       string,
    dep_airport    string,
    arr_time       string,
    arr_airport    string
) PARTITIONED BY (month_id) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

CREATE TABLE IF NOT EXISTS output_concat
(
    sex            string,
    age_range      string,
    bill_range     string,
    live_city_id   string,
    belong_city_id string,
    dep_time       string,
    dep_airport    string,
    arr_time       string,
    arr_airport    string,
    usr_cnt        int
) PARTITIONED BY (month_id) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

--step1 按月获取流量数据，并过滤
INSERT OVERWRITE TABLE hdy_temp_available_gprs_data PARTITION (dt = '${yyyyMMdd}')
SELECT phone_no,
       city_id,
       start_datetime,
       visit_area_code,
       roam_type
FROM dw_gprs_cdr_d
WHERE visit_area_code != -1
  and stat_time = '${yyyyMMdd}';

--step2 用户出省漫游情况
--流量表里的城市id和城市维表里的城市id是否匹配？roamcity_id -> city_code

INSERT OVERWRITE TABLE hdy_temp_roam_prov PARTITION (month_id = ${yyyyMM} )
SELECT a.phone_no,
       a.city_id,
       a.dep_time,
       b.prov_id as dep_prov_id,
       a.arr_time,
       c.prov_id as arr_prov_id
FROM (
         SELECT phone_no,
                city_id,
                start_datetime as dep_time,
                roamcity_id    as dep_prov_cap,
                lead(start_datetime) OVER (
                    PARTITION BY phone_no
                    ORDER BY start_datetime ASC
                    )          as arr_time,
                lead(roamcity_id) OVER (
                    PARTITION BY phone_no
                    ORDER BY start_datetime ASC
                    )          as arr_prov_cap
         FROM hdy_temp_available_gprs_data
         WHERE substr(dt,1,6) = ${yyyyMM}
     )a
         LEFT JOIN city_info b ON a.dep_prov_cap = b.city_code
         LEFT JOIN city_info c ON a.arr_prov_cap = c.city_code
WHERE b.prov_id != c.prov_id;


--进港
    --飞机直飞
INSERT INTO TABLE hdy_temp_air_direct_in_analysis PARTITION (dt = '${yyyyMMdd}')
SELECT c.phone_no,
       d.dep_city_id,
       d.dep_airport,
       c.dep_time,
       c.arr_time,
       d.arr_city_id,
       c.arr_airport,
       c.stay_place --到达之后停留地
FROM (
         SELECT a.phone_no,
                b.dep_prov_id,
                b.dep_time,
                a.dept_hour   as arr_time,
                a.roamcity_id as arr_city_id,
                a.szm         as arr_airport,
                a.active_id   as stay_place
         FROM xl_iogx_mode2 a
                  INNER JOIN hdy_temp_roam_prov b ON a.phone_no = b.phone_no
             and a.dept_hour = substr(b.arr_time, 12, 2)
         WHERE a.dt = ${yyyyMMdd}
           and (b.dt = ${yyyyMMdd} or b.dt = ${yyyyMMdd-1})
           and b.arr_prov_id = '51'
           and a.io_flag = 1
     ) c
         LEFT JOIN (SELECT start_airport, dep_time1, arr_time1, end_airpprt hdy_local_air_time_info) d ON c.arr_airport = d.arr_airport
    and c.dep_prov_id = d.dep_prov_id
    and c.dep_time = hour(d.dep_time)
    and c.arr_time = hour(d.arr_time)
WHERE d.op_date = substr(c.dep_time,1,6);

    --空地中转
INSERT OVERWRITE TABLE hdy_temp_transfer_in_analysis PARTITION(dt = ${yyyyMMdd})
SELECT s.phone_no, dep_city,dep_rail_station,dep_time, arr_time, arr_city,arr_rail_station
FROM (SELECT phone_no, hdy_res_xl_air_inout_detail) s LEFT JOIN hdy_res_xl_train_inout_detail t ON s.phone_no = t.phone_no
AND



    --出港
INSERT OVERWRITE TABLE hdy_temp_air_direct_out_analysis PARTITION (dt = ${yyyyMMdd})
SELECT c.phone_no,
       c.dep_time,
       c.dep_city_id,
       c.dep_airport,
       c.arr_time,
       d.arr_city_id,
       d.arr_airport,
       c.stay_place --出发之前停留地
FROM (
         SELECT a.phone_no,
                a.dept_hour   as dep_time,
                a.roamcity_id as dep_city_id,
                a.szm         as dep_airport,
                b.arr_time,
                b.arr_prov_id,
                a.active_id   as stay_place
         FROM xl_iogx_mode2 a
                  INNER JOIN clean_gprs b ON a.phone_no = b.phone_no
             and a.dept_hour = substr(b.arr_time, 12, 2)
         WHERE a.dt = ${yyyyMMdd}
           and (b.dt = ${yyyyMMdd+1} or b.dt = ${yyyyMMdd})
           and b.arr_prov_id = '51'
           and a.io_flag = 0
     ) c
         LEFT JOIN hdy_local_air_time_info d
                   ON c.dep_airport = d.dep_airport
                       and c.arr_prov_id = d.arr_prov_id
                       and c.dep_time = hour(d.dep_time)
                       and c.arr_time = hour(d.arr_time)
WHERE d.op_date = substr(c.dep_time,1,6);

    --地空中转

--合并进出港，归属地，常住地
INSERT OVERWRITE TABLE hdy_temp_in_out_combined PARTITION (month_id = '${month_id}')
SELECT a.phone_no,
       b.city_id as live_city_id,
       c.city_id as belong_city_id,
       a.dep_time,
       a.dep_city_id,
       a.dep_airport,
       a.arr_time,
       a.arr_city_id,
       a.arr_airport,
       a.in_out_label
FROM (
         SELECT phone_no,
                dep_time,
                dep_city_id,
                dep_airport,
                arr_time,
                arr_city_id,
                arr_airport,
                stay_place,
                1 as in_out_label
         FROM hdy_temp_air_in_analysis
         WHERE substr(dt, 1, 6) = '${yyyyMM}'
         UNION ALL
         SELECT phone_no,
                dep_time,
                dep_city_id,
                dep_airport,
                arr_time,
                arr_city_id,
                arr_airport,
                stay_place,
                0 as in_out_label
         FROM hdy_temp_air_out_analysis
         WHERE substr(dt, 1, 6) = '${yyyyMM}'
     ) a
         LEFT JOIN dw_position_resident_yx_m b ON a.phone_no = b.phone_no
         LEFT JOIN dw_user_info_m c ON a.phone_no = c.phone_no
WHERE a.phone_no IS NOT NULL;

--输出宽表
INSERT OVERWRITE TABLE output_detailed PARTITION (month_id = '${month_id}')
SELECT a.phone_no,
       b.sex,
       b.age,
       c.bill_fee,
       a.live_city_id,
       a.belong_city_id,
       a.dep_time,
       a.dep_airport,
       a.arr_time,
       a.arr_airport
FROM hdy_temp_in_out_combined a
         LEFT JOIN dw_user_age_sex_m b ON a.phone_no = b.phone_no
         LEFT JOIN dw_bill_user_m c ON a.phone_no = c.phone_no;

--聚合结果
INSERT OVERWRITE TABLE output_concat PARTITION (month_id = '${month_id}')
SELECT sex,
       CASE
           WHEN age BETWEEN 0 AND 20 THEN 1
           WHEN age BETWEEN 21 AND 30 THEN 2
           WHEN age BETWEEN 31 AND 40 THEN 3
           WHEN age BETWEEN 41 AND 50 THEN 4
           WHEN age BETWEEN 51 AND 60 then 5
           WHEN age > 60 THEN 6
           END as age_range,
       CASE
           WHEN bill_fee BETWEEN 0 AND 50 THEN 1
           WHEN bill_fee BETWEEN 51 AND 100 THEN 2
           WHEN bill_fee BETWEEN 101 AND 150 THEN 3
           WHEN bill_fee BETWEEN 151 AND 200 THEN 4
           WHEN bill_fee > 200 THEN 5
           END AS bill_range,
       live_city_id,
       belong_city_id,
       dep_time,
       dep_airport,
       arr_time,
       arr_airport,
       count(phone_no)
FROM output_detailed
WHERE phone_no IS NOT NULL
   OR sex IS NOT NULL
   OR age IS NOT NULL
   OR bill_fee IS NOT NULL
    AND (
              live_city_id IS NOT NULL
              OR belong_city_id IS NOT NULL
          )
   OR dep_time IS NOT NULL
   OR dep_airport IS NOT NULL
   OR arr_time IS NOT NULL
   OR arr_airport IS NOT NULL
GROUP BY sex,
         age_range,
         bill_range,
         live_city_id,
         belong_city_id,
         dep_time,
         dep_airport,
         arr_time,
         arr_airport;