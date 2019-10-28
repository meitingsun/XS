#!/bin/bash

source /etc/profile
source ~/.bash_profile

export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom

preDay=`date -d '2 days ago' +%Y-%m-%d`

prefix='/user/root/dn_success_stat/tables'

tName='tqxs.T_TQ_INDEX_POWER_ALL' 

hive -e "
insert overwrite  directory '$prefix/T_TQ_INDEX_POWER_ALL/$preDay' row format delimited fields terminated by '|' NULL DEFINED AS '0'

select 
	tg_no,
	data_date,
	fgcl,
	fhl,
	avg_fzl
from sjzl.index_power_all 
"

sqoop export -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table $tName \
-m 1 \
--columns tg_no,data_date,fgcl,fhl,avg_fzl \
--connection-param-file /opt/gcxm/jobs/conn-params.txt \
--export-dir $prefix/T_TQ_INDEX_POWER_ALL/$preDay \
--fields-terminated-by '|' \
-- --default-character-set=utf-8

