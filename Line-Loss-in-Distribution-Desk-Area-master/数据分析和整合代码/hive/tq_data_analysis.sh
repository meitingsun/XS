#!/bin/bash

hive -e "

use sjzl;

create table index_factor_$1 as
select 
    tg_no,
    data_date,
    avg(data) factor_avg,
    max(data) factor_max,
    min(data) factor_min
from result_factor_$1
where phase_flag = '0' and data is not null and data > 75 and data <= 100
group by tg_no, data_date;


create table index_cur_$1 as
select 
    a.tg_no,
    a.data_date,
    max(a.sx) as cur_max,
    min(a.sx) as cur_min,
    avg(a.sx) as cur_avg
from (select 
        tg_no,
        data_date,
        count(data) count,
        case when (max(data)-avg(data)) > (avg(data)-min(data)) then (max(data)-avg(data))/avg(data) else (avg(data)-min(data))/avg(data) end as sx
    from result_cur_$1
    where data is not null and phase_flag in ('1','2','3') and data > 0
    group by tg_no, data_date, datetime) a
where a.count = 3
group by tg_no, data_date;


create table index_power_$1 as
select 
    a.tg_no,
    a.data_date,
    (max1-min1)/max1 as fgcl,
    avg1/max1 as fhl,
    sqrt(avg1*avg1 + avg5*avg5) as avg_fzl
from (select 
        tg_no,
        data_date,
        max(data) as max1,
        min(data) as min1,
        avg(data) as avg1
    from result_power_$1
    where data is not null and phase_flag = 1
    group by tg_no, data_date) a join 
    (select 
        tg_no,
        data_date,
        max(data) as max5,
        min(data) as min5,
        avg(data) as avg5
    from result_power_$1
    where data is not null and phase_flag = 5
    group by tg_no, data_date) b
on a.tg_no = b.tg_no and a.data_date = b.data_date;


create table index_$1 as
select 
	a.tg_no,
    a.data_date,
	a.factor_avg,
	a.factor_max,
	a.factor_min,
	b.cur_max,
	b.cur_min,
	b.cur_avg,
	c.fgcl,
	c.fhl,
	c.avg_fzl
from index_factor_$1 a join index_cur_$1 b join index_power_$1 c
on a.tg_no = b.tg_no and b.tg_no = c.tg_no and a.data_date = b.data_date and b.data_date = c.data_date
where b.cur_max is not NULL and c.fgcl is not NULL and a.factor_avg is not NULL;
	
drop table index_factor_$1;
drop table index_cur_$1;
drop table index_power_$1;

"

echo "数据分析完毕"



