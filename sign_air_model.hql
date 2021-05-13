CREATE TABLE IF NOT EXISTS hdy_local_airport_prov_city_info(
    iata_code string,
    air_name string,
    city_name string,
    prov_name string,
    lng string,
    lat string,
    lng84 string,
    lat84 string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC;

load data local inpath '机场信息表.csv' into table hdy_local_airport_prov_city_info;

CREATE TABLE IF NOT EXISTS `hdy_temp_airport_info`
(
    `lac_id` string,
    `cell_id` string
)
    COMMENT '机场附近基站'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE IF NOT EXISTS `hdy_temp_xl_iogx_usr`
(
    `phone_no` string COMMENT '用户标识'
)
    COMMENT '机场出现的目标用户'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_detail`
(
    `time_id`       string COMMENT '记录时间',
    `phone_no`      string COMMENT '用户标示',
    `city_id`       string COMMENT '归属地',
    `roamcity_id`   string COMMENT '漫游地',
    `is_airport_flag` string COMMENT '机场位置标示',
    `lac_id`        string,
    `cell_id` string
)
    COMMENT '目标用户三天信令数据'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_scope2`
(
    `time_id`     string COMMENT '记录时间',
    `phone_no`    string COMMENT '用户标识',
    `city_id`     string COMMENT '归属地',
    `roamcity_id` string COMMENT '城市'
)
    COMMENT '用户出现在机场的第一条数据'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_scope3`
(
    `time_id`     string COMMENT '记录时间',
    `phone_no`    string COMMENT '用户标识',
    `city_id`     string COMMENT '归属地',
    `roamcity_id` string COMMENT '漫游城市'
)
    COMMENT '用户出现在机场的最后一条数据'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_scope5`
(
    `phone_no` string COMMENT '用户标识'
)
    COMMENT '目标用户机场出现在机场前10个小时之前是否目标城市有数据'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_scope6`
(
    `phone_no` string COMMENT '用户标识'
)
    COMMENT '目标用户离开机场后10个小时之后是否目标城市有数据'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_scope7`
(
    `phone_no`    string COMMENT '用户标识',
    `city_id`     string COMMENT '归属地',
    `roamcity_id` string COMMENT '漫游城市',
    `time_id`     string COMMENT '记录时间',
    `stay_time`   string COMMENT '停留时间'
)
    COMMENT '进港用户'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_scope8`
(
    `phone_no`    string COMMENT '用户标识',
    `city_id`     string COMMENT '归属地',
    `roamcity_id` string COMMENT '漫游城市',
    `time_id`     string COMMENT '记录时间',
    `stay_time`   string COMMENT '停留时间'
)
    COMMENT '出港用户'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_temp_xl_iogx_active`
(
    `phone_no` string COMMENT '用户id',
    `area_id`  string COMMENT '区县',
    `dist_id`  string COMMENT '乡镇',
    `io_flag`  string COMMENT '进出港标识'
)
    COMMENT '进出港用驻留地判断'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_res_xl_air_inout_detail`
(
    `dept_hour`   string COMMENT '离开时刻',
    `phone_no`    string COMMENT '用户标示',
    `age` string,
    `sex` string,
    `bill_fee` string,
    `city_id`     string COMMENT '归属地',
    `active_id`   string COMMENT '机场前后驻留地',
    `roamcity_id` string COMMENT '出发地',
    `stay_dur`    string COMMENT '停留时间',
    `io_flag`     string COMMENT '进出港标示'
)
    COMMENT '汇总进港用户和出港用户'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

CREATE TABLE if not exists `hdy_res_xl_air_inout_agg`
(
    `city_id`   string COMMENT '归属地',
    `live_id`   string COMMENT '常住地',
    `area_id`   string COMMENT '常驻区县',
    `active_id` string COMMENT '到达机场前后区域',
    `dept_hour` string COMMENT '离开时刻',
    `dept_city` string COMMENT '离开机场城市',
    `stay_dur`  string COMMENT '停留时长',
    `usr_cnt`   string COMMENT '用户数',
    `io_flag`   string COMMENT '进出港标示'
)
    COMMENT '按输出口径聚合数据'
    PARTITIONED BY (
        `dt` string,
        `szm` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS orc;

--圈出四川省机场附近基站
insert overwrite table hdy_temp_airport_info partition (dt = '${yyyyMMdd}', szm)
select distinct p.lac_id, p.cell_id, p.szm
from (
         select s.lac_id, s.cell_id, t.iata_code as szm
         from ${bts_sc_all_day} s
                  inner join hdy_local_airport_prov_city_info t on s.city_name = t.city_name
         where t.prov_name = '四川省'
           and round(
                           2 * Asin(
                               Sqrt(
                                           power(
                                                   sin(
                                                               (
                                                                   s.lat * 3.141592625 / 180.0 - t.lat84 * 3.141592625 / 180.0
                                                                   ) / 2
                                                       ),
                                                   2
                                               ) +
                                           cos(s.lat * 3.141592625 / 180.0) * cos(t.lat84 * 3.141592625 / 180.0) *
                                           power(
                                                   sin(
                                                               (
                                                                   s.lng * 3.141592625 / 180.0 - t.lng84 * 3.141592625 / 180.0
                                                                   ) / 2
                                                       ),
                                                   2
                                               )
                                   )
                           ) * 6378.137 * 10000,
                           1
                   ) / 10000 - t.radius <= 0) p;

-- 1.圈出机场覆盖范围用户
--信令表需要调整
insert overwrite table hdy_temp_xl_iogx_usr partition (dt = '${yyyyMMdd}', szm)
select distinct t.phone_no, s.szm
from ${sichuan_xl} t
         left join hdy_temp_airport_info s on t.lac_id = s.lac_id and t.cell_id = s.cell_id
    and t.stat_month = ${yyyyMM}
    and t.stat_time = ${yyyyMMdd}
    and s.lac_id is not null
    and s.cell_id is not null
    and t.phone_no != ''
    and substr(t.phone_no, 1, 3) != '106'
    and substr(t.phone_no, 1, 3) != '144';

--2.反溯四川机场覆盖目标用户3天详单
insert overwrite table hdy_temp_xl_iogx_detail partition (dt = '${yyyyMMdd}', szm)
select r.time_id,
       r.phone_no,
       r.city_id,     --归属地
       r.roamcity_id, --当前所在城市
       case
           when (i.lac_id is not null and i.cell_id is not null)
               then 1
           else 0
           end as is_airport_flag,
       r.lac_id,
       r.cell_id,
       r.szm
from (
         select t.start_datetime as time_id,
                t.phone_no,
                t.city_id,                       --归属地
                t.loc_city       as roamcity_id, --当前所在地
                t.lac_id,
                t.cell_id,
                s.szm
         from ${sichuan_xl} t
                  left join (
             select phone_no,szm
             from hdy_temp_xl_iogx_usr
             where dt = ${yyyyMMdd}
         ) s on t.phone_no = s.phone_no
         where s.phone_no is not null
           and t.stat_month = ${yyyyMM}
           and t.stat_time >= ${yyyyMMdd-1}
           and t.stat_time <= ${yyyyMMdd+1}
     ) r
         left join (
    select lac_id, cell_id, szm
    from hdy_temp_airport_info
) i on r.lac_id = i.lac_id and r.cell_id = i.cell_id and r.szm = i.szm;

--3.用户出现在机场范围接收指令第一条记录
insert overwrite table hdy_temp_xl_iogx_scope2 partition (dt = '${yyyyMMdd}', szm)
select p.time_id,
       p.phone_no,
       p.city_id,
       p.roamcity_id,
       p.szm
from (
         select t.time_id,
                t.phone_no,
                t.city_id,
                t.roamcity_id,
                row_number() over (
                    partition by t.phone_no
                    order by t.time_id asc
                    ) rns,
                t.szm
         from hdy_temp_xl_iogx_detail t
         where t.dt = ${yyyyMMdd}
           and t.is_airport_flag = 1
     ) p
where p.rns = 1;

--4.用户出现在机场范围记录最后一条记录[JOIN广东机场基站维表]
insert overwrite table hdy_temp_xl_iogx_scope3 partition (dt = ${yyyyMMdd}, szm)
select p.time_id,
       p.phone_no,
       p.city_id,
       p.roamcity_id,
       p.szm
from (
         select t.time_id,
                t.phone_no,
                t.city_id,
                t.roamcity_id,
                row_number() over (
                    partition by phone_no
                    order by time_id desc
                    ) rns,
                t.szm
         from hdy_temp_xl_iogx_detail t
         where t.dt = ${yyyyMMdd}
           and t.airport_flag = 1
     ) p
where p.rns = 1;

--5.目标用户出现在机场10个小时之前是否目标城市有数据[JOIN成都机场基站维表]
insert overwrite table hdy_temp_xl_iogx_scope5 partition (dt = ${yyyyMMdd}, szm)
select distinct t.phone_no, t.szm
from (
         select time_id,
                phone_no,
                city_id,
                roamcity_id,
                is_airport_flag,
                lac_id,
                cell_id,
                szm
         from hdy_temp_xl_iogx_detail
         where dt = ${yyyyMMdd}
     ) t
         left join(
    select time_id as end_time,
           phone_no,
           roamcity_id,
           from_unixtime(
                   (unix_timestamp(time_id) - 36000),
                   'yyyy-MM-dd HH:mm:ss'
               )   as start_time
    from hdy_temp_xl_iogx_scope2
    where dt = ${yyyyMMdd}
) s on t.phone_no = s.phone_no and t.szm = s.szm
where t.time_id > s.start_time
  and t.time_id < s.end_time
  and t.roamcity_id = s.roamcity_id
  and s.phone_no is not null;

--6.目标用户离开机场10个小时之后是否目标城市有数据
insert overwrite table hdy_temp_xl_iogx_scope6 partition (dt = ${yyyyMMdd}, szm)
select distinct t.phone_no
from (
         select time_id,
                phone_no,
                city_id,
                roamcity_id,
                is_airport_flag,
                lac_id,
                cell_id,
                szm
         from hdy_temp_xl_iogx_detail
         where dt = ${yyyyMMdd}
     ) t
         left join(
    select time_id as start_time,
           phone_no,
           roamcity_id,
           from_unixtime(
                   (unix_timestamp(time_id) + 36000),
                   'yyyy-MM-dd HH:mm:ss'
               )   as end_time,
           szm
    from hdy_temp_xl_iogx_scope3
    where dt = ${yyyyMMdd}
) s on t.phone_no = s.phone_no and t.szm = s.szm
where t.time_id > s.start_time
  and t.time_id < s.end_time
  and t.roamcity_id = s.roamcity_id
  and s.phone_no is not null;

--7.进港判断
insert overwrite table hdy_temp_xl_iogx_scope7 partition (dt = ${yyyyMMdd}, szm)
select i.phone_no,
       i.city_id,
       i.roamcity_id,
       i.time_id,
       i.stay_time,
       i.szm
from (
         select r.phone_no,
                r.city_id,
                r.roamcity_id,
                r.time_id,
                round(
                            (
                                unix_timestamp(s.time_id) - unix_timestamp(r.time_id)
                                ) / 3600,
                            2
                    ) as stay_time,
                r.szm
         from (
                  select t.phone_no,
                         t.city_id,
                         t.roamcity_id,
                         t.time_id
                  from (
                           select time_id,
                                  phone_no,
                                  city_id,
                                  roamcity_id,
                                  szm
                           from hdy_temp_xl_iogx_scope2
                           where dt = ${yyyyMMdd}
                       ) t
                           left join (
                      select phone_no, szm
                      from hdy_temp_xl_iogx_scope5
                      where dt = ${yyyyMMdd}
                  ) s on t.phone_no = s.phone_no and t.szm = s.szm
                  where s.phone_no is null
              ) r
                  left join (
             select time_id,
                    phone_no,
                    city_id,
                    roamcity_id,
                    szm
             from hdy_temp_xl_iogx_scope3
             where dt = ${yyyyMMdd}
         ) s on r.phone_no = s.phone_no and r.szm = s.szm
         where s.phone_no is not null
     ) i
where i.stay_time between 0.4 and 2.6;

--8.出港判断
insert overwrite table hdy_temp_xl_iogx_scope8 partition (dt = ${yyyyMMdd}, szm)
select i.phone_no,
       i.city_id,
       i.roamcity_id,
       i.time_id,
       i.stay_time,
       i.szm
from (
         select r.phone_no,
                r.city_id,
                r.roamcity_id,
                r.time_id,
                round(
                            (
                                unix_timestamp(r.time_id) - unix_timestamp(s.time_id)
                                ) / 3600,
                            2
                    ) as stay_time,
                r.szm
         from (
                  select t.phone_no,
                         t.city_id,
                         t.roamcity_id,
                         t.time_id
                  from (
                           select time_id,
                                  phone_no,
                                  city_id,
                                  roamcity_id,
                                  szm
                           from hdy_temp_xl_iogx_scope3
                           where dt = ${yyyyMMdd}
                       ) t
                           left join (
                      select phone_no,
                             szm
                      from hdy_temp_xl_iogx_scope6
                      where dt = ${yyyyMMdd}
                  ) s on t.phone_no = s.phone_no and t.szm = s.szm
                  where s.phone_no is null
              ) r
                  left join (
             select time_id,
                    phone_no,
                    city_id,
                    roamcity_id,
                    szm
             from hdy_temp_xl_iogx_scope2
             where dt = ${yyyyMMdd}
         ) s on r.phone_no = s.phone_no and r.szm = s.szm
         where s.phone_no is not null
     ) i
where i.stay_time between 1.0 and 3.5;

--
--9.出港用户常住地判断 针对出港旅客，取早上4-6点所在的基站小区所在区县。
INSERT overwrite TABLE hdy_temp_xl_iogx_active PARTITION (dt = ${yyyyMMdd}, szm)
SELECT w.phone_no,
       w.dist_nam,
       '',
       0 AS flag,
       w.szm
FROM (
         SELECT p.phone_no,
                p.dist_nam,
                row_number() OVER (
                    PARTITION BY p.phone_no
                    ORDER BY stay_dur DESC
                    ) AS rns,
                p.szm
         FROM (
                  SELECT r.phone_no,
                         r.dist_nam,
                         max(unix_timestamp(r.time_id)) - min(unix_timestamp(r.time_id)) stay_dur,
                         r.szm
                  FROM (
                           SELECT t.phone_no,
                                  i.dist_nam,
                                  t.time_id,
                                  t.szm
                           FROM (
                                    SELECT time_id,
                                           phone_no,
                                           city_id,
                                           roamcity_id,
                                           airport_flag,
                                           lac_id,
                                           cell_id,
                                           szm
                                    FROM hdy_temp_xl_iogx_detail
                                    WHERE dt = ${yyyyMMdd}
                                ) t
                                    INNER JOIN county_cell_info i ON t.lac_id = i.lac_id and t.cell_id = i.cell_id --county_cell_info 区县基站维表
                       ) r
                           LEFT JOIN (
                      SELECT phone_no,
                             city_id,
                             roamcity_id,
                             time_id,
                             stay_time,
                             szm
                      FROM hdy_temp_xl_iogx_scope8
                      WHERE dt = ${yyyyMMdd}
                  ) s ON r.phone_no = s.phone_no and r.szm = s.szm
                  WHERE s.phone_no IS NOT NULL
                    AND r.time_id BETWEEN concat(to_date(s.time_id), ' 00:00:00') AND concat(to_date(s.time_id), ' 06:00:00')
                  GROUP BY r.phone_no,
                           r.dist_nam
              ) p
     ) w
WHERE w.rns = 1;

--10.进港用户目的地 进港旅客的目的地取晚上10点后的驻留地，如果10点以后进港的旅客取当天的最后基站小区所在位置。针对出港旅客，取早上4-6点所在的基站小区所在区县。
INSERT INTO TABLE hdy_temp_xl_iogx_active PARTITION (dt = ${yyyyMMdd}, szm)
SELECT w.phone_no,
       w.dist_nam,
       '',
       1 AS flag,
       w.szm
FROM (
         SELECT p.phone_no,
                p.dist_nam,
                row_number() OVER (
                    PARTITION BY p.phone_no
                    ORDER BY stay_dur DESC
                    ) AS rns,
                p.szm
         FROM (
                  SELECT r.phone_no,
                         r.dist_nam,
                         max(unix_timestamp(r.time_id)) - min(unix_timestamp(r.time_id)) stay_dur,
                         r.szm
                  FROM (
                           SELECT t.phone_no,
                                  i.dist_nam,
                                  t.time_id,
                                  t.szm
                           FROM (
                                    SELECT time_id,
                                           phone_no,
                                           city_id,
                                           roamcity_id,
                                           is_airpo_flag,
                                           lac_id,
                                           cell_id,
                                           szm
                                    FROM hdy_temp_xl_iogx_detail
                                    WHERE dt = ${yyyyMMdd}
                                ) t
                                    INNER JOIN county_cell_info i ON t.lac_id = i.lac_id and t.cell_id = i.cell_id --county_cell_info 区县基站维表
                       ) r
                           LEFT JOIN (
                      SELECT phone_no,
                             city_id,
                             roamcity_id,
                             time_id,
                             stay_time,
                             szm
                      FROM hdy_temp_xl_iogx_scope7
                      WHERE dt = ${yyyyMMdd}
                  ) s ON r.phone_no = s.phone_no and r.szm = s.szm
                  WHERE s.phone_no IS NOT NULL
                    AND r.time_id BETWEEN case
                                              when hour(s.time_id) >= 18
                                                  then concat(date_add(s.time_id, 1), ' 00:00:00')
                                              else from_unixtime(
                                                      (unix_timestamp(s.time_id) + 7200),
                                                      'yyyy-MM-dd HH:mm:ss'
                                                  )
                      end
                      AND case
                              when hour(s.time_id) >= 18 then concat(date_add(s.time_id, 1), ' 06:00:00')
                              else from_unixtime(
                                      (unix_timestamp(s.time_id) + 64800),
                                      'yyyy-MM-dd HH:mm:ss'
                                  )
                          end
                  GROUP BY r.phone_no,
                           r.dist_nam
              ) p
     ) w
WHERE w.rns = 1;

---11.汇总进出港用户
insert overwrite table hdy_res_xl_air_inout_detail partition (dt = ${yyyyMMdd}, szm)
select substr(b.time_id, 12, 2) dept_hour,
       b.phone_no,
       e.age,
       e.sex,
       w.bill_fee,
       b.city_id,
       d.active_id,
       b.roamcity_id,
       b.stay_time,
       0                        io_flag,
       b.szm
from (
         select phone_no,
                city_id,
                roamcity_id,
                time_id,
                stay_time,
                szm
         from hdy_temp_xl_iogx_scope8
         where dt = ${yyyyMMdd}
     ) b
         left join (
    select phone_no,
           active_id,
           szm
    from hdy_temp_xl_iogx_active
    where dt = ${yyyyMMdd}
      and io_flag = 0
) d on b.phone_no = d.phone_no and b.szm = d.szm
         left join dw_user_age_sex_m e
                   on b.phone_no = e.phone_no
         left join (select t.phone_no, t.user_id, s.bill_fee
                    from ${dw_bill_user_m} s
                             left join dw_position_resident_yx_m t
                                       on s.user_id = t.user_id) w on b.phone_no = w.phone_no
where date_format(time_id, 'yyyyMMdd') = ${yyyyMMdd}
  and d.phone_no is not null
union
select substr(i.time_id, 12, 2) dept_hour,
       i.phone_no,
       l.age,
       l.sex,
       m.bill_fee,
       i.city_id,
       g.active_id as           live_county_id,
       i.roamcity_id,
       i.stay_time,
       1                        io_flag,
       i.szm
from (
         select phone_no,
                city_id,
                roamcity_id,
                time_id,
                stay_time,
                szm
         from hdy_temp_xl_iogx_scope7
         where dt = ${yyyyMMdd}
     ) i
         left join (
    select phone_no,
           area_id,
           szm
    from hdy_temp_xl_iogx_active
    where dt = ${yyyyMMdd}
      and io_flag = 1
) g on i.phone_no = g.phone_no and i.szm = g.szm
         left join dw_user_age_sex_m l
                   on i.phone_no = l.phone_no
         left join (select t.phone_no, t.user_id, s.bill_fee
                    from ${dw_bill_user_m} s
                             left join dw_position_resident_yx_m t
                                       on s.user_id = t.user_id) m on i.phone_no = m.phone_no
where date_format(time_id, 'yyyyMMdd') = ${yyyyMMdd}
  and g.phone_no is not null;

--12.按输出口径聚合数据
insert overwrite table hdy_res_xl_air_inout_agg partition (dt = ${yyyyMMdd}, szm)
select t.city_id,
       s.city_nam,
       s.area,
       t.active_id,
       t.dept_hour,
       t.roamcity_id as dept_city,
       case
           when t.stay_dur > 0
               and t.stay_dur <= 1 then '0-1'
           when t.stay_dur > 1
               and t.stay_dur <= 2 then '1-2'
           when t.stay_dur > 2
               and t.stay_dur <= 3 then '2-3'
           when t.stay_dur > 3
               and t.stay_dur <= 4 then '3-4'
           when t.stay_dur > 4
               and t.stay_dur <= 5 then '4-5'
           else '>5'
           end       as stay_dur,
       count(1)         usr_cnt,
       t.io_flag,
       t.szm
from (
         select dept_hour,
                phone_no,
                city_id,
                active_id,
                roamcity_id,
                stay_dur,
                io_flag,
                szm
         from hdy_res_xl_air_inout_detail
         where dt = ${yyyyMMdd}
     ) t
         left join (
    select phone_no,
           city_id
    from ${dw_position_resident_yx_m} --常住表
    where month = ${yyyyMMdd}
) s on t.phone_no = s.usr_nbr
where t.dt = ${yyyyMMdd}
group by t.city_id,
         s.city_nam,
         s.area,
         t.active_id,
         t.dept_hour,
         t.roamcity_id,
         case
             when t.stay_dur > 0
                 and t.stay_dur <= 1 then '0-1'
             when t.stay_dur > 1
                 and t.stay_dur <= 2 then '1-2'
             when t.stay_dur > 2
                 and t.stay_dur <= 3 then '2-3'
             when t.stay_dur > 3
                 and t.stay_dur <= 4 then '3-4'
             when t.stay_dur > 4
                 and t.stay_dur <= 5 then '4-5'
             else '>5'
             end,
         t.io_flag,
         t.szm;