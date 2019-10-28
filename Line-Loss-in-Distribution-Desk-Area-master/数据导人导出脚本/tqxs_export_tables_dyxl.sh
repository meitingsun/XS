#!/bin/bash

source /etc/profile
source ~/.bash_profile

export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom

preDay=`date -d '2 days ago' +%Y-%m-%d`

prefix='/user/root/dn_success_stat/tables'

tName='tqxs.T_TQ_DYXL' 

hive -e "
insert overwrite  directory '$prefix/T_TQ_DYXL/$preDay' row format delimited fields terminated by '|' NULL DEFINED AS '0'

select byq.obj_id tran_obj_id ,
	byq.sbbm tran_sbbm,
	byq.sbmc tran_sbmc,
	dyxl.obj_id obj_id,
	dyxl.sbbm sbbm,
	dyxl.sbmc sbmc,
	dyxl.xh xh,
	dyxl.cd cd,
	dyxl.jmj jmj,
	dyxl.cl cl,
	dyxl.datatype datatype
from
(select a.obj_id,a.sbbm,a.sbmc,a.ywdw,a.yxzt,a.syxz from sg_data_sharing_warehouse.t_sb_zwyc_zsbyq a where a.update_time=DATE_SUB(CURRENT_DATE(),2)
union all 
select b.obj_id,b.sbbm,b.sbmc,b.ywdw,b.yxzt,b.syxz from sg_data_sharing_warehouse.t_sb_znyc_pdbyq b where b.update_time=DATE_SUB(CURRENT_DATE(),2)) byq
join sg_data_sharing_warehouse.t_sb_dysb_dyxl xl on byq.obj_id = xl.ssbyq and xl.update_time=DATE_SUB(CURRENT_DATE(),2)
join (
select dx.obj_id,dx.sbbm,dx.sbmc,dx.ssxl,dx.xh,dx.dxcd as cd,dxgg as jmj,null as cl,'dx' as datatype
from sg_data_sharing_warehouse.t_sb_dysb_dydx dx where dx.update_time=DATE_SUB(CURRENT_DATE(),2)
union all
select dld.obj_id,dld.sbbm,dld.sbmc,dld.ssxl,dld.xh,dld.cd,dld.dljm as jmj,xxcl as cl,'dld' as datatype
from sg_data_sharing_warehouse.t_sb_dysb_dydld dld where dld.update_time=DATE_SUB(CURRENT_DATE(),2)
) dyxl on xl.obj_id = dyxl.ssxl
where byq.ywdw in (
'8a0a8b8c4cf9f689014cf9f6bbe70b14',
'8a0a8b8c4cf9f689014cf9f6babe048f',
'8a0a8b8c4cf9f689014cf9f6babe060c',
'8a0a8b8c4cf9f689014cf9f6bbe709e9',
'8a0a8b8c4cf9f689014cf9f6babe0675',
'8a0a8b8c4cf9f689014cf9f6bbe70b40',
'8a0a8b8c4cf9f689014cf9f6bbe70b72',
'8a0a8b8c4cf9f689014cf9f6babe04cc',
'8a0a8b8c4cf9f689014cf9f6babe0502',
'8a0a8b8c4cf9f689014cf9f6bbe70b92',
'8a0a8b8c4cf9f689014cf9f6bbe70b26',
'8a0a8b8c4cf9f689014cf9f6bbe70a5c',
'8a0a8b8c4cf9f689014cf9f6bbe70a11',
'8a0a8b8c4cf9f689014cf9f6babe06eb',
'8a0a8b8c4cf9f689014cf9f6babe04ea',
'8a0a8b8c4cf9f689014cf9f6babe04a6',
'8a0a8b8c4cf9f689014cf9f6bbe70b5a',
'8a0a8b8c4cf9f689014cf9f6bd9d0bcc',
'8a0a8b8c4cf9f689014cf9f6babe060d',
'8a0a8b8c4cf9f689014cf9f6babe062f',
'8a0a8b8c4cf9f689014cf9f6babe0655',
'8a0a8b8c4cf9f689014cf9f6bbe709f1',
'8a0a8b8c4cf9f689014cf9f6babe04db'
) and byq.syxz = '03' and byq.yxzt = '20';"

sqoop export -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table $tName \
-m 1 \
--columns tran_obj_id,tran_sbbm,tran_sbmc,obj_id,sbbm,sbmc,xh,cd,jmj,cl,datatype \
--connection-param-file /opt/gcxm/jobs/conn-params.txt \
--export-dir $prefix/T_TQ_DYXL/$preDay \
--fields-terminated-by '|' \
-- --default-character-set=utf-8

