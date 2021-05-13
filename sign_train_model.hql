CREATE TABLE IF NOT EXISTS `hdy_temp_railway_info`
(
    `lac_id`  string,
    `cell_id` string,
    `szm`     string
)
    COMMENT '高铁附近基站'
    PARTITIONED BY (`dt` string, `city` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS `hdy_temp_xl_iogx_usr`
(
    `phone_no` string,
    `szm`      string,
    `lac_id`   string,
    `cell_id`  string
)
    COMMENT '机场出现用户'
    PARTITIONED BY (
        `dt` string,
        `city` string
        )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS `hdy_temp_xl_iogx_stay_place`
(
    `phone_no` string,
    `szm`      string,
    `stay_dur` int
)
    PARTITIONED BY (dt string, city string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS `hdy_res_xl_train_inout_detail`
(
    `phone_no`         string,
    `dep_city`         string,
    `dep_rail_station` string,
    `dep_time`         string,
    `arr_time`         string,
    `arr_city`         string,
    `arr_rail_station` string
)
    PARTITIONED BY (month_id string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC;

--如果局方有更精准点位识别，用局方的识别算法
--1. 找出高铁附近基站

INSERT OVERWRITE TABLE hdy_temp_railway_info PARTITION (dt = '${yyyyMMdd}', city)
SELECT DISTINCT p.lac_id, p.cell_id, p.szm, p.city_id
FROM (SELECT s.city_id, t.station_code AS szm, s.lac_id, s.cell_id
      FROM ${bts_sc_all_day} s
               INNER JOIN hdy_local_airport_prov_city_info t on s.city_name = t.city_name
      WHERE t.prov_name = '四川省'
        AND round(2 * Asin(Sqrt(power(sin((s.lat * 3.141592625 / 180.0 - t.lat84 * 3.141592625 / 180.0) / 2), 2) +
                                cos(s.lat * 3.141592625 / 180.0) * cos(t.lat84 * 3.141592625 / 180.0) * power(
                                        sin((s.lng * 3.141592625 / 180.0 - t.lng84 * 3.141592625 / 180.0) / 2), 2))) *
                  6378.137 * 10000, 1) / 10000 - t.radius <= 0) p;

--2.基站覆盖人群
INSERT OVERWRITE TABLE hdy_temp_xl_iogx_usr PARTITION (dt = '{yyyyMMdd}', city)
SELECT a.phone_no, b.szm, b.lac_id, b.cell_id, a.city_id
FROM ${sichuan_xl} a
         LEFT JOIN hdy_temp_railway_info b ON a.lac_id = b.lac_id
    AND a.cell_id = b.cell_id AND a.stat_month = ${yyyyMM} AND a.stat_time = ${yyyyMMdd} AND
                                              b.lac_id IS NOT NULL AND b.cell_id IS NOT NULL AND
                                              a.phone_no != '' AND
                                              substr(a.phone_no, 1, 3) != '106' AND
                                              substr(a.phone_no, 1, 3) != '144';

--3. 每个人在高铁站出现的时间
INSERT OVERWRITE TABLE hdy_temp_xl_iogx_stay_place PARTITION (dt = '${yyyyMMdd}', city)
SELECT c.phone_no,
       c.szm,
       sum(unix_timestamp(c.end_time, 'yyyyMMddHHmmss') - unix_timestamp(c.start_time, 'yyyyMMddHHmmss')) /
       60 as stay_dur,
       c.city_id
FROM (
         SELECT a.phone_no,
                b.start_datetime                                                               AS start_time,
                a.szm,
                b.start_lac,
                b.start_ci,
                lead(b.start_datetime) over
                    (PARTITION BY a.phone_no ORDER BY b.start_datetime ASC)                    AS end_time,
                lead(b.start_lac) over (PARTITION BY a.phone_no ORDER BY b.start_datetime ASC) AS end_lac,
                lead(b.start_ci)
                     over (PARTITION BY a.phone_no ORDER BY b.start_datetime ASC)              AS end_ci,
                a.city_id
         FROM hdy_temp_xl_iogx_usr a
                  LEFT JOIN ${sichuan_xl} b ON a.lac_id = b.start_lac
             AND a.loc_city = b.loc_city AND a.cell_id = b.start_ci) c
WHERE stat_time = ${yyyyMMddHHmmss}
  AND start_lac = end_lac
  AND start_ci = end_ci
GROUP BY c.phone_no, c.loc_city, c.szm
HAVING sum(unix_timestamp(c.end_time, 'yyyyMMddHHmmss') - unix_timestamp(c.start_time, 'yyyyMMddHHmmss')) / 60 > 10;

--4.用户地面轨迹
INSERT OVERWRITE TABLE hdy_res_xl_train_inout_detail PARTITION (month_id = '${yyyyMM}')
SELECT a.phone_no, dep_city, a.dep_rail_station, a.dep_time, a.arr_time, a.arr_city, a.arr_rail_station
FROM (
         SELECT phone_no,
                first_value(loc_city) OVER (PARTITION BY phone ORDER BY start_datetime)              as dep_city,
                first_value(szm) OVER (PARTITION BY phone_no ORDER BY start_datetime ASC)            as dep_rail_station,
                first_value(start_datetime) OVER (PARTITION BY phone_no ORDER BY start_datetime ASC) as dep_time,
                last_value(start_datetime) OVER (PARTITION BY phone_no ORDER BY start_datetime ASC) as arr_time,
                last_value(loc_city) OVER (PARTITION BY phone_no ORDER BY start_datetime ASC)       as arr_city,
                last_value(szm) OVER (PARTITION BY phone_no ORDER BY start_datetime ASC)            AS arr_rail_station
         FROM hdy_temp_xl_iogx_stay_place
         WHERE unix_timestamp(stat_time) BETWEEN (unix_timestamp(stat_time) - 20 * 3600) AND (unix_timestamp(stat_time) + 20 * 3600)
         ORDER BY phone_no,start_datetime ASC
    )a ;

INSERT OVERWRITE TABLE hdy_res_xl_train_inout_agg PARTITION (month_id = ${yyyyMM})
SELECT phone_no, city_id as live_city_id, city_id as belong_city_id, sex, age, bill




