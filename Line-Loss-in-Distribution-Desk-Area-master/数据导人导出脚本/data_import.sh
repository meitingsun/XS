#!/bin/bash


sqoop import \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table R_DATA_MP \
--target-dir /user/sjzl/r_data_mp \
--fields-terminated-by "\001" \
--hive-drop-import-delims  \
-m1


hive -e "

use sjzl;


create external table r_data_mp(
    id string,
    meter_id string
)
row format delimited fields terminated by '\001'
lines terminated by '\n' stored as textfile;


load data inpath '/user/sjzl/r_data_mp/' into table r_data_mp;
"


sqoop import \
--connect "jdbc:oracle:thin:@10.138.7.19:21521:orcl" \
--username "tqxs" \
--password Tqxs_190318 \
--table T_TQ_CONS \
--target-dir /user/sjzl/t_tq_cons \
--fields-terminated-by "\001" \
--hive-drop-import-delims  \
-m1


hive -e "

use sjzl;

create external table t_tq_cons(
    cons_id string,
    tg_no string,
    cons_name string,
    cons_type string,
    cons_cap string,
    cons_addr string,
    ps_date string,
    volt_code string,
    mp_id string,
    tg_distance string,
    tg_line string
)
row format delimited fields terminated by '\001'
lines terminated by '\n' stored as textfile;


load data inpath '/user/sjzl/t_tq_cons/' into table t_tq_cons;

"


