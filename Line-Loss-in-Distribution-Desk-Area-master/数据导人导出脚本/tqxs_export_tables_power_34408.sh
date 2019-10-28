#!/bin/bash

source /etc/profile
source ~/.bash_profile

export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom

preDay=`date -d '2 days ago' +%Y-%m-%d`

prefix='/user/root/dn_success_stat/tables'

tName='gcxm.T_TQ_INDEX_POWER_20190615' 

hive -e "
insert overwrite  directory '$prefix/T_TQ_INDEX_POWER_20190615/$preDay' row format delimited fields terminated by '|' NULL DEFINED AS '0'

select 
	tg_no,
	data_date,
	max max1,
	min min1,
	avg avg1
from sjzl.index_power_20190615 
"

sqoop export -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "gcxm" \
--password Gcxm_12040 \
--table $tName \
-m 1 \
--columns tg_no,data_date,max1,min1,avg1 \
--connection-param-file /opt/gcxm/jobs/conn-params.txt \
--export-dir $prefix/T_TQ_INDEX_POWER_20190615/$preDay \
--fields-terminated-by '|' \
-- --default-character-set=utf-8

