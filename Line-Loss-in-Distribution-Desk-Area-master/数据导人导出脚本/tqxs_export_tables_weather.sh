#!/bin/bash

source /etc/profile
source ~/.bash_profile

export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom

preDay=`date -d '2 days ago' +%Y-%m-%d`

prefix='/user/root/dn_success_stat/tables'

tName='tqxs.T_WEATHER' 

hive -e "
insert overwrite  directory '$prefix/T_WEATHER/$preDay' row format delimited fields terminated by '|' NULL DEFINED AS '0'

select 
	aa.pubtime,
	aa.city,
	aa.effdate,
	aa.weather12,
	aa.weather24,
	aa.temperature1,
	aa.temperature2,
	aa.crsj,
	aa.update_time
from sg_data_sharing_warehouse.t_weatherforecast aa
"

sqoop export -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table $tName \
-m 1 \
--columns pubtime,city,effdate,weather12,weather24,temperature1,temperature2,crsj,update_time \
--connection-param-file /opt/gcxm/jobs/conn-params.txt \
--export-dir $prefix/T_WEATHER/$preDay \
--fields-terminated-by '|' \
-- --default-character-set=utf-8

