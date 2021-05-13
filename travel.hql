CREATE IF NOT EXISTS TABLE target_cells(
	place_name string COMMENT '旅游景点'，
	lac_id string COMMENT '基站'，
	cell_id string COMMENT '扇区'
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
STORED as textfile;

CREATE IF NOT EXISTS TABLE potential_travelers(
	phone_no string,
	place_name string,
	time string,
	stay_min string
)
PARTITIONED BY (day_id string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
STORED as textfile;

CREATE IF NOT EXISTS TABLE add_restrictions(
	phone_no string, 
	place_name string
)
PARTITIONED BY (month_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
STORED as textfile;

CREATE IF NOT EXISTS TABLE true_customers(
	phone_no string,
	belong_city_id string,
	live_county_id string,
	sex string,
	age string, 
	bill_fee string,
	place_name string,
	stay_min string
)
PARTITION BY (month_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS textfile;

CREATE IF NOT EXISTS TABLE true_customers_calculate(
	belong_city_id string,
	live_county_id string,
	sex string,
	age_range string, 
	bill_range string,
	place_name string,
	stay_min string,
	usr_cnt string
)
PARTITION BY (month_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS textfile;

--根据实际ci确定旅游区域
--如果不可以确定具体ci则删除ci字段
INSERT OVERWRITE  TABLE target_cells
SELECT a.place_name, b.lac_id, b.cell_id
    FROM trip_places a LEFT JOIN bts_sc_all_day b ON a.city_name = b.city_name
    WHERE round(
		2 * Asin(
			Sqrt(
				power(
					sin(
						(
							a.lat * 3.141592625 / 180.0 -b.lat* 3.141592625 / 180.0
						) / 2
					),
					2
				) + cos(a.lat * 3.141592625 / 180.0) * cos(30.56256217 * 3.141592625 / 180.0) * power(
					sin(
						(
							a.lng * 3.141592625 / 180.0 -b.lng * 3.141592625 / 180.0
						) / 2
					),
					2
				)
			)
		) * 6378.137 * 10000,
		1
	) / 10000 < a.radius;


INSERT OVERWRITE TABLE potential_travelers PARTITION(day_id = '${date}')
SELECT w.phone_no,
    w.place_name,
	w.time,
    sum(w.stay_min) as stay_min
FROM (
        SELECT m.phone_no,    
			m.start_lac,
			m.start_ci,
			n.place_name,
			n.time,
			sum(( unix_timestamp(m.start_datetime, 'yyyyMMddHHmmss') - unix_timestamp(m.end_datetime, 'yyyyMMddHHmmss')) / 60) OVER (PARTITION BY m.phone_no, m.start_lac, m.start_ci ORDER BY start_datetime) as stay_min
FROM sichuan_xl m
    LEFT JOIN target_cells n ON m.start_lac = n.lac_id
    and m.start_ci = n.cell_id
WHERE m.phone_no in (SELECT distinct phone_no FROM xl_iogx_mode2) 
	and m.stat_month = '${month}'
	and m.stat_time = '${day}' 
    and m.start_lac = m.end_lac
    and m.start_ci = m.end_ci
) w
GROUP BY w.phone_no,
    w.place_name,
HAVING sum(w.stay_min) > w.time;



INSERT OVERWRITE TABLE add_restrictions PARTITION(month_id = '${month}')
SELECT phone_no, place_name FROM potential_travelers WHERE stay_min >= 360 and substr(day_id,1,6) = '${sm}' GROUP BY phone_no, place_name HAVING count(day_id) > 20
UNION
SELECT phone_no, place_name FROM potential_travelers WHERE substr(day_id,1,6) = '${sm}' GROUP BY phone_no, place_name HAVING count(day_id) > 10;

INSERT OVERWRITE TABLE true_customers PARTITION(month_id = '${month}')
SELECT c.phone_no, d.belong_city_id,d.live_county_id, d.sex, d.age, d.bill_fee, c.place_name, c.stay_min FROM potential_travelers c 
LEFT JOIN add_restrictions b ON c.phone_no = b.phone_no 
LEFT JOIN xl_iogx_mode2 d ON c.phone_no = d.phone_no
WHERE substr(c.day_id,1,6) = b.month_id and b.phone_no IS NULL;

INSERT OVERWRITE TABLE true_customers_calculate PARTITION (month_id = '${month}')
SELECT belong_city_id, live_county_id, sex, 
		case when  age <=20 and age >0  then 1
            when age <=30 and age >20 then 2
            when age <=40 and age >30 then 3
            when age <=50 and age > 40  then 4
            when age <=60 and age > 50 then 5
            when age >60 then 6 end as age_range,
       case when bill_fee > 0 and bill_fee <=50 then 1
            when bill_fee > 50 and bill_fee <=100 then 2
            when bill_fee >  100 and bill_fee <= 150 then 3
            when bill_fee > 150 and bill_fee < 200 then 4
            when bill_fee > 200 then 5 end as bill_range,
			place_name, stay_min,
			count(phone_no) usr_cnt 
			group by 
			 belong_city_id, live_county_id, sex, 
		case when  age <=20 and age >0  then 1
            when age <=30 and age >20 then 2
            when age <=40 and age >30 then 3
            when age <=50 and age > 40  then 4
            when age <=60 and age > 50 then 5
            when age >60 then 6 end ,
       case when bill_fee > 0 and bill_fee <=50 then 1
            when bill_fee > 50 and bill_fee <=100 then 2
            when bill_fee >  100 and bill_fee <= 150 then 3
            when bill_fee > 150 and bill_fee < 200 then 4
            when bill_fee > 200 then 5 end,
			place_name, stay_min