#!/bin/bash

hive -e "

use sjzl;


create table result_factor_curve_all as 
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201801
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201802
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201803
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201804
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201805
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201806
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201807
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201808
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201809
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201810
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201811
union all
select  data_id	, data_date , phase_flag , datetime, data from data_factor_201812


create table result_cur_curve_all as 
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201801
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201802
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201803
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201804
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201805
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201806
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201807
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201808
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201809
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201810
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201811
union all
select  data_id	, data_date , phase_flag , datetime, data from data_cur_201812


create table result_power_curve_all as 
select  data_id	, data_date , phase_flag , datetime, data from data_power_201801
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201802
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201803
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201804
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201805
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201806
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201807
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201808
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201809
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201810
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201811
union all
select  data_id	, data_date , phase_flag , datetime, data from data_power_201812


"


echo "数据合并成功！"




