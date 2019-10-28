#!/bin/bash

source /etc/profile
source ~/.bash_profile

export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom

preDay=`date -d '2 days ago' +%Y-%m-%d`

prefix='/user/root/dn_success_stat/tables'

tName='tqxs.T_TQ_POWER_ALL' 

hive -e "
insert overwrite  directory '$prefix/T_TQ_POWER_ALL/$preDay' row format delimited fields terminated by '|' NULL DEFINED AS '0'

select t.tg_no,t.data_date,avg(t.data)
from sjzl.result_power_201904 t
where t.phase_flag = 1 
group by t.tg_no,t.data_date;
"

sqoop export -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table $tName \
-m 1 \
--columns  tg_no,data_date,power_avg \
--connection-param-file /opt/gcxm/jobs/conn-params.txt \
--export-dir $prefix/T_TQ_POWER_ALL/$preDay \
--fields-terminated-by '|' \
-- --default-character-set=utf-8

