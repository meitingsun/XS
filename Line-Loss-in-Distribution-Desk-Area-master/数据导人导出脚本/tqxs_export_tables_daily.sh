#!/bin/bash

source /etc/profile
source ~/.bash_profile

export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom

preDay=`date -d '2 days ago' +%Y-%m-%d`

DateTime=`date +%Y%m%d`

num=3

prefix='/user/root/dn_success_stat/tables'

tName='tqxs.T_TQ_CAL_ALL' 


hive -e "
insert overwrite  directory '$prefix/T_TQ_CAL_ALL/$preDay' row format delimited fields terminated by '|' NULL DEFINED AS '0'

select t0.tg_no,t4.caldt,sum(t4.cal_pvalue) cal_sum,count(t4.cal_pvalue) cal_count
from root.dn_g_tg t0 
left join root.dn_c_mp t1 on t0.tg_id = t1.tg_id
left join root.dn_c_meter_mp_rela t2 on t1.mp_id = t2.mp_id
left join root.dn_c_meter t3 on t2.meter_id = t3.meter_id
left join root.dn_daily_full_result t4 on t3.meter_id = t4.meter_id
where t0.update_time=DATE_SUB(CURRENT_DATE(),1)
and t1.update_time=DATE_SUB(CURRENT_DATE(),1)
and t2.update_time=DATE_SUB(CURRENT_DATE(),1)
and t3.update_time=DATE_SUB(CURRENT_DATE(),1) 
and t4.cal_pvalue is not null and t4.cal_pvalue !=0
and substr(t0.org_no,0,5) in ('34401','34412','34413','34417')  
and t4.caldt >= to_date('2019-4-26') and t4.caldt < to_date('2019-5-1')
group by t0.tg_no,t4.caldt 
order by t0.tg_no,t4.caldt
"

sqoop export -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table $tName \
-m 1 \
--columns tg_no,caldt,cal_sum,cal_count \
--connection-param-file /opt/gcxm/jobs/conn-params.txt \
--export-dir $prefix/T_TQ_CAL_ALL/$preDay \
--fields-terminated-by '|' \
-- --default-character-set=utf-8

