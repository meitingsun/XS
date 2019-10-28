
set hive.cli.print.header=true;
set hive.resultset.use.nuique.column.names=false;


 
  -----------------------------------城乡特征---------------------------------------
  
/*UPDATE T_TQ_BASE A SET A.TG_CITY_TYPE =NULL;*/

/*merge into T_TQ_BASE bb
using(
select tt.tg_id,(case when tt.S1=0 and tt.S2=0 then '特殊边远山区' when tt.S1=0 and tt.S3=0 
                     then '农村' when tt.S2=0 and tt.S3=0 then '城市' else '混合' end) as area  from 
(select t2.tg_id,
      SUM(CASE WHEN T1.URBAN_RURAL_FLAG ='01' THEN 1 ELSE 0 END) AS S1,
      SUM(CASE WHEN T1.URBAN_RURAL_FLAG ='02' THEN 1 ELSE 0 END) AS S2,
      SUM(CASE WHEN T1.URBAN_RURAL_FLAG ='03' THEN 1 ELSE 0 END) AS S3
  from cacher01.c_cons t1
  left join cacher01.c_mp t2 
    on t1.cons_id = t2.cons_id and t2.type_code = '01'
 where t1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no)  and  t1.cons_sort_code in ('03', '02')
 group by t2.tg_id
 order by t2.tg_id) tt ) aa on (aa.tg_id = bb.tg_id)
 when matched then
   update set bb.tg_city_type = aa.area;*/


  -----------------------------------台区基础数据---------------------------------------
--01 gongbian 02 zhuanbian
--亳州 34417 滁州34413  合肥34401 黄山34412

/*insert into T_TQ_BASE(TG_ID,TG_NO,TG_NAME,TG_CAP,ORG_NO,INST_ADDR,PUB_PRIV_FLAG,RUN_STATUS_CODE) */
select a.tg_id,a.tg_no,a.tg_name,a.tg_cap,a.org_no,a.inst_addr,a.pub_priv_flag,a.run_status_code from cacher01.g_tg a 
where a.pub_priv_flag='01' and a.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no);

select * from cacher01.o_org o 
--where o.org_name like '%黄山%';
where o.p_org_no = '34401';
select * from cacher01.p_code


  -----------------------------------变压器型号---------------------------------------

/*create table temp_20190319 as
select b.tg_id,byq.xh from cacher01.g_tran b 
JOIN (select aa.obj_id,aa.xh from scyw.t_sb_znyc_pdbyq aa union all select bb.obj_id,bb.xh from scyw.t_sb_zwyc_zsbyq bb)  byq on b.pms_equip_id = byq.obj_id
where exists(select 1 from cacher01.g_tg tg where b.tg_id = tg.tg_id and tg.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no)
            );

merge into T_TQ_BASE aa
using (select a.tg_id,a.xh from temp_20190319 a
where a.rowid in (select max(rowid) from temp_20190319 b group by b.tg_id)) bb on (aa.tg_id = bb.tg_id)
            when matched then update set aa.model_no = bb.xh;*/
 
 
  -----------------------------------台区用户总报装容量---------------------------------------w
/*merge into T_TQ_BASE aa
using ( select tt1.tg_id,sum(tt3.contract_cap) cap from  cacher01.g_tg tt1 left join cacher01.c_mp tt2 on tt1.tg_id = tt2.tg_id and tt2.type_code = '01'
  left join cacher01.c_cons tt3 on tt2.cons_id = tt3.cons_id 
  where tt1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no) group by tt1.tg_id) bb on (aa.tg_id = bb.tg_id and bb.cap is not null) 
            when matched then update set aa.tg_cons_cap = bb.cap;*/
       
	   
  -----------------------------------台区用户报装容量---------------------------------------
  -- 居民200
  
/*  select tt1.tg_id,(case when tt3.elec_type_code) cap from  cacher01.g_tg tt1 left join cacher01.c_mp tt2 on tt1.tg_id = tt2.tg_id 
  left join cacher01.c_cons tt3 on tt2.cons_id = tt3.cons_id 
  where tt1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no) group by tt1.tg_id;*/
        
		
 -----------------------------------台区各类型用户报装容量---------------------------------------
/* merge into T_TQ_BASE aa           
 using ( select tt1.tg_id,sum((case when tt4.class_name = '工业' then tt3.contract_cap end)) cap_gy,sum((case when tt4.class_name = '农业' then tt3.contract_cap end)) cap_ny,
 sum((case when tt4.class_name = '商业' then tt3.contract_cap end)) cap_sy, sum((case when tt4.class_name = '居民' then tt3.contract_cap end)) cap_jm,
 sum((case when tt4.class_name = '非居民' then tt3.contract_cap end)) cap_fjm
 from  cacher01.g_tg tt1 left join cacher01.c_mp tt2 on tt1.tg_id = tt2.tg_id and tt2.type_code = '01'
  left join cacher01.c_cons tt3 on tt2.cons_id = tt3.cons_id left join t_elec_type tt4 on tt3.elec_type_code = tt4.type_code
  where tt1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no) group by tt1.tg_id) bb on (aa.tg_id = bb.tg_id)
            when matched then update set 
              aa.tg_cons_gy_cap = nvl(bb.cap_gy,0),
              aa.tg_cons_ny_cap = nvl(bb.cap_ny,0),
              aa.tg_cons_sy_cap = nvl(bb.cap_sy,0),
              aa.tg_cons_qt_cap = nvl(bb.cap_fjm,0),
              aa.tg_cons_jm_cap = nvl(bb.cap_jm,0);*/
       
	   
 -----------------------------------台区220v/380v电压用户数量---------------------------------------
 
/*merge into T_TQ_BASE aa
using (select tt1.tg_id,sum(case when tt3.volt_code='AC02202' then tt3.contract_cap end) cap01,sum(case when tt3.volt_code='AC03802' then tt3.contract_cap end) cap02 
from  cacher01.g_tg tt1 left join cacher01.c_mp tt2 on tt1.tg_id = tt2.tg_id 
left join cacher01.c_cons tt3 on tt2.cons_id = tt3.cons_id
where tt1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
               --and o1.org_id = a.org_id
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no) group by tt1.tg_id) bb on (aa.tg_id = bb.tg_id)
             when matched then update set aa.tg_v_cap01 = bb.cap01,aa.tg_v_cap02 = bb.cap02;*/

			 
 -----------------------------------台区用户 ---------------------------------------
 
 /*insert into T_TQ_CONS(CONS_ID,TG_NO,CONS_NAME,CONS_CAP,CONS_ADDR,PS_DATE,VOLT_CODE,MP_ID)*/
 select a3.cons_id,
        a1.tg_no,
        a3.cons_name,
        a3.contract_cap,
        a3.elec_addr,
        a3.ps_date,
        a3.elec_type_code,
        a2.mp_id
 from cacher01.g_tg a1 left join cacher01.c_mp a2 on a1.tg_id = a2.tg_id left join cacher01.c_cons a3 on a2.cons_id = a3.cons_id
where a1.pub_priv_flag='01' and a1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no);


select * from cacher01.c_cons a where a.cons_sort_code = '02';

select * from temp_20190319;

select * from T_TQ_CONS;

SELECT * FROM CACHER01.P_CODE A WHERE A.CODE_TYPE = 'VOLT_CODE';


---- 更新用户类别
update t_tq_cons a set a.cons_type = (select b.class_name from cacher01.c_cons c left join  t_elec_type b on c.elec_type_code = b.type_code where a.cons_id = c.cons_id)
----------------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
SELECT * FROM t_weather a where a.city='合肥' order by a.effdate desc ,a.pubtime desc;

select * from T_TQ_DATA t order by t.report_date,t.tg_no;


select * from cacher01.g_tg a1 left join cacher01.c_mp a2 on a1.tg_id = a2.tg_id and a2.type_code = '02'
left join cacher01.c_meter_mp_rela a3 on a2.mp_id = a3.mp_id
left join cacher01.r_data_mp a4 on a3.meter_id = a4.meter_id
left join e_mp_factor_curve a5 on a4.id=a5.data_id 
where a1.org_no in ('34401',
'3440101',
'3440102',
'3440103',
'3440104',
'3441104',
'34412',
'3441201',
'3441202',
'3441203',
'3441204',
'3441205',
'34413',
'3441301',
'3441302',
'3441303',
'3441304',
'3441305',
'3441306',
'34417',
'3441701',
'3441702',
'3441703'
) and a5.data_date>'20161231' and a5.phase_flag='0'；
--------------------------------------------------气温--------------------------------------------------------------------------


select * from t_weather ;

--create table T_ORG AS
select o.org_no,o.org_name,o.p_org_no,o.org_type from cacher01.o_org o where o.org_type <'05' and o.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
             START WITH O1.Org_No in ('34401','34412','34413','34417')
            CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no);
         
/*merge into T_TQ_DATA_TEMP_20190329_1 tt
using (
      select aa.tg_no,c.date_date,c.temperature1,c.temperature2  
      from(
        select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
        union all
        select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
      ) aa
      join t_org b on aa.org_no = b.org_no
      join t_weather c on b.jc = c.city
)  mm on (tt.tg_no = mm.tg_no and tt.report_date = mm.date_date)
when matched then
  update set tt.max_temp = mm.temperature2,tt.min_temp = mm.temperature1,tt.avg_temp = (mm.temperature2+mm.temperature1)/2;*/
            
            
            
            
--------------------------------------------光伏上网电量---------------------------------------------------------------------------------  

 select * from DNUSER.DN_DAILY_FULL_RESULT t where t.caldt < to_date('2019-02-23','yyyy-mm-dd');
 
 select * from root.DN_DAILY_FULL_RESULT t order by t.caldt;

select * from cacher01.fc_gc;

select * from 

-- a1.gc_id,a4.meter_id,a4.t_factor,a2.tg_id
--create table TEMP_20190327 AS
select a1.gc_id,a4.meter_id,a4.t_factor,a2.tg_id from cacher01.fc_gc a1 join cacher01.c_mp a2 on a1.gc_id = a2.cons_id  and a2.usage_type_code in ('1102','1101')
join cacher01.c_meter_mp_rela a3 on a2.mp_id = a3.mp_id 
join cacher01.c_meter a4 on a3.meter_id = a4.meter_id
where  a1.org_no in (select o1.org_no
              FROM cacher01.o_org O1
             WHERE O1.Org_Type < '05'
              START WITH O1.Org_No in ('34401','34412','34413','34417')
             CONNECT BY PRIOR O1.ORG_no = O1.P_ORG_no);
          
select a1.gc_id,a4.meter_id,a4.t_factor,a2.tg_id from sg_data_sharing_warehouse.fc_gc a1 
join root.dn_c_mp a2 on a1.gc_id = a2.cons_id  and a2.usage_type_code in ('1102','1101')
join root.dn_c_meter_mp_rela a3 on a2.mp_id = a3.mp_id 
join root.dn_c_meter a4 on a3.meter_id = a4.meter_id
where substr(a1.org_no,0,5) in ('34401','34412','34413','34417') 
and a1.update_time=DATE_SUB(CURRENT_DATE(),1) 
and a2.update_time=DATE_SUB(CURRENT_DATE(),1) 
and a3.update_time=DATE_SUB(CURRENT_DATE(),1)
and a4.update_time=DATE_SUB(CURRENT_DATE(),1);

select a1.gc_id,a4.meter_id,a4.t_factor,a2.tg_id from sg_data_sharing_warehouse.fc_gc a1 
join root.dn_c_mp a2 on a1.gc_id = a2.cons_id  and a2.usage_type_code in ('1102','1101')
join root.dn_c_meter_mp_rela a3 on a2.mp_id = a3.mp_id 
join root.dn_c_meter a4 on a3.meter_id = a4.meter_id
where substr(a1.org_no,0,5) in ('34401','34412','34413','34417') 
and a1.update_time=DATE_SUB(CURRENT_DATE(),1) 
and a2.update_time=DATE_SUB(CURRENT_DATE(),1) 
and a3.update_time=DATE_SUB(CURRENT_DATE(),1)
and a4.update_time=DATE_SUB(CURRENT_DATE(),1);


select t1.tg_id,(t2.tday_pvalue-t2.yday_pvalue)*t1.t_factor power,t1.meter_id,t2.caldt
from TEMP_20190327 t1 
join DNUSER.DN_DAILY_FULL_RESULT t2 on t1.meter_id = t2.meter_id where t2.caldt < to_date('2019-02-24','yyyy-mm-dd');


truncate table T_TQ_FACTOR_CUR_POWER;

SELECT * FROM T_TQ_FACTOR_CUR_POWER;




------------用采功率因数、电流、电压计算指标---------
create table T_TQ_INDEX_ALL
(
  tg_no      VARCHAR2(32),
  data_date  VARCHAR2(32),
  factor_avg VARCHAR2(32),
  factor_max VARCHAR2(32),
  factor_min VARCHAR2(32),
  cur_max    VARCHAR2(32),
  cur_min    VARCHAR2(32),
  cur_avg    VARCHAR2(32),
  fgcl       VARCHAR2(32),
  fhl        VARCHAR2(32),
  avg_fzl    VARCHAR2(32),
  xsl        NUMBER
);

select * from T_TQ_INDEX_ALL;


select count(*) from T_TQ_INDEX_ALL;

select count(distinct tg_no) from T_TQ_INDEX_ALL;

select distinct tg_no from T_TQ_INDEX_ALL order by tg_no;

--create table index_all_1 as 
select count(*) from T_TQ_INDEX_ALL where tg_no < '1220685581';

--create table index_all_2 as 
select count(*) from T_TQ_INDEX_ALL where tg_no <= '2043112405' and tg_no >= '1220685581';

--create table index_all_3 as 
select count(*) from T_TQ_INDEX_ALL where tg_no > '2043112405';


----------------------------------------根据台区分组计算各连续指标-------------------------
--create table t_tq_data_base_tq_20190403_1 as 
select t.tg_no tg_no, 
       chg_date,
       tg_cons_cap,
       tg_cons_jm_cap,
       tg_cons_gy_cap,
       tg_cons_sy_cap,
       tg_cons_qt_cap,
       tg_v_cap01,
       tg_v_cap02,
       avg(t.xsl) avg_xsl,
       stddev(t.xsl) std_xsl,
       avg(t.factor_avg) factor_avg,
       stddev(t.factor_avg) std_factor_avg,
       avg(t.factor_max) factor_max,
       stddev(t.factor_max) std_factor_max,
       avg(t.factor_min) factor_min,
       stddev(t.factor_min) std_factor_min,
       avg(t.cur_max) cur_max,
       stddev(t.cur_max) std_cur_max,
       avg(t.cur_min) cur_min,
       stddev(t.cur_min) std_cur_min,
       avg(t.cur_avg) cur_avg,
       stddev(t.cur_avg) std_cur_avg,
       avg(t.fgcl) fgcl,
       stddev(t.fgcl) std_fgcl,
       avg(t.fhl) fhl,
       stddev(t.fhl) std_fhl,
       avg(t.avg_fzl) avg_fzl,
       stddev(t.avg_fzl) std_avg_fzl,
       t2.xsl_type xsl_type_max,
       count(1) total from T_TQ_DATA_BASE_20190403_1 t left join(
       select a.tg_no, a.xsl_type
       from (
            select t.tg_no, t.xsl_type, rank() over (partition by t.tg_no order by count(1) desc) rank_type 
            from T_TQ_DATA_BASE_20190403_1 t group by t.tg_no, t.xsl_type) a
       where a.rank_type = 1
       ) t2 on t.tg_no = t2.tg_no group by t.tg_no,t.chg_date,
       t.tg_cons_cap,
       t.tg_cons_jm_cap,
       t.tg_cons_gy_cap,
       t.tg_cons_sy_cap,
       t.tg_cons_qt_cap,
       t.tg_v_cap01,
       t.tg_v_cap02,
       t2.xsl_type;
       
       
     select a.tg_no, a.xsl_type
     from (
   select t.tg_no, t.xsl_type, rank() over (partition by t.tg_no order by count(1) desc) rank_type from T_TQ_DATA_BASE_20190403_1 t group by t.tg_no, t.xsl_type) a
   where a.rank_type = 1;
       
   --2066231132 1  2065381234 2 2065381794 0
       select t.xsl_type,count(1) from T_TQ_DATA_BASE_20190403_1 t where t.tg_no ='2066231132' group by t.xsl_type;
   
   
   
   
   ------------------------------------------原数据宽表----------------------------
create table t_tq_data_base_20190403_1 as 
SELECT a2.tg_cap,
       a2.chg_date,
       a2.tg_cons_cap,
       a2.tg_cons_jm_cap,
       a2.tg_cons_gy_cap,
       a2.tg_cons_sy_cap,
       a2.tg_cons_qt_cap,
       a2.tg_v_cap01,
       a2.tg_v_cap02,
       a2.tg_city_type,
       mm.date_date,
       mm.temperature1,
       mm.temperature2,
       (mm.temperature1+mm.temperature2)/2 avg_temp,
       a1.*
      FROM t_tq_data_temp a1 left join t_tq_base a2 on a1.tg_no = a2.tg_no
      left join (
            select aa.tg_no,c.date_date,c.temperature1,c.temperature2  
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm  on a1.tg_no = mm.tg_no where to_date(a1.data_date,'YYYY-MM-DD') = mm.date_date;








------------------根据温度计算相关指标---------
--create table t_tq_data_base_temp_20190403_1 as 
select ceil(t.avg_temp) avg_temp,
       avg(t.xsl) avg_xsl,
       stddev(t.xsl) std_xsl,
       avg(t.factor_avg) factor_avg,
       stddev(t.factor_avg) std_factor_avg,
       avg(t.factor_max) factor_max,
       stddev(t.factor_max) std_factor_max,
       avg(t.factor_min) factor_min,
       stddev(t.factor_min) std_factor_min,
       avg(t.cur_max) cur_max,
       stddev(t.cur_max) std_cur_max,
       avg(t.cur_min) cur_min,
       stddev(t.cur_min) std_cur_min,
       avg(t.cur_avg) cur_avg,
       stddev(t.cur_avg) std_cur_avg,
       avg(t.fgcl) fgcl,
       stddev(t.fgcl) std_fgcl,
       avg(t.fhl) fhl,
       stddev(t.fhl) std_fhl,
       avg(t.avg_fzl) avg_fzl,
       stddev(t.avg_fzl) std_avg_fzl,
       count(1) total
from T_TQ_DATA_BASE_20190403_1 t group by ceil(t.avg_temp) order by ceil(t.avg_temp);


------------------根据时间计算相关指标------------
--create table t_tq_data_base_date_20190403_1 as 
select ceil(t.chg_date/90) avg_chg_date_90,
       avg(t.xsl) avg_xsl,
       stddev(t.xsl) std_xsl,
       avg(t.factor_avg) factor_avg,
       stddev(t.factor_avg) std_factor_avg,
       avg(t.factor_max) factor_max,
       stddev(t.factor_max) std_factor_max,
       avg(t.factor_min) factor_min,
       stddev(t.factor_min) std_factor_min,
       avg(t.cur_max) cur_max,
       stddev(t.cur_max) std_cur_max,
       avg(t.cur_min) cur_min,
       stddev(t.cur_min) std_cur_min,
       avg(t.cur_avg) cur_avg,
       stddev(t.cur_avg) std_cur_avg,
       avg(t.fgcl) fgcl,
       stddev(t.fgcl) std_fgcl,
       avg(t.fhl) fhl,
       stddev(t.fhl) std_fhl,
       avg(t.avg_fzl) avg_fzl,
       stddev(t.avg_fzl) std_avg_fzl,
       count(1) total
from T_TQ_DATA_BASE_20190403_1 t group by ceil(t.chg_date/90) order by ceil(t.chg_date/90);



--------------------分析温度和线损率源数据---------------------
--create table t_tq_data_temp_xsl_20190404_1 as
select mm.tg_no,mm.date_date,t.xsl,mm.city,mm.weather12,mm.weather24,mm.temperature1,mm.temperature2,mm.avg_temp
from (select aa.tg_no,c.date_date,c.city,c.weather12,c.weather24,c.temperature1,c.temperature2,(c.temperature1+c.temperature2)/2 avg_temp
      from(
        select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
        union all
        select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
      ) aa
      join t_org b on aa.org_no = b.org_no
      join t_weather c on b.jc = c.city) mm 
      left join T_TQ_DATA t on  t.report_date = mm.date_date and t.tg_no = mm.tg_no;
      
	  
-----------温度----------
select ceil(t.avg_temp),
       avg(t.xsl) avg_xsl,
       max(t.xsl) max_xsl,
       min(t.xsl) min_xsl,
       count(t.xsl) count_xsl,
       stddev(t.xsl) std_xsl
from t_tq_data_temp_xsl_20190404_1 t
where t.xsl is not null and t.xsl >= 0 and t.xsl <= 12
group by ceil(t.avg_temp)
order by ceil(t.avg_temp);


-----------天气12----------
select t.weather12,
       avg(t.xsl) avg_xsl,
       max(t.xsl) max_xsl,
       min(t.xsl) min_xsl,
       count(t.xsl) count_xsl,
       stddev(t.xsl) std_xsl
from t_tq_data_temp_xsl_20190404_1 t
where t.xsl is not null and t.xsl >= 0 and t.xsl <= 12
group by t.weather12;


-----------天气24----------
select t.weather24,
       avg(t.xsl) avg_xsl,
       max(t.xsl) max_xsl,
       min(t.xsl) min_xsl,
       count(t.xsl) count_xsl,
       stddev(t.xsl) std_xsl
from t_tq_data_temp_xsl_20190404_1 t
where t.xsl is not null and t.xsl >= 0 and t.xsl <= 12
group by t.weather24;


-----------城市----------
select t.city,
       avg(t.xsl) avg_xsl,
       max(t.xsl) max_xsl,
       min(t.xsl) min_xsl,
       count(t.xsl) count_xsl,
       stddev(t.xsl) std_xsl
from t_tq_data_temp_xsl_20190404_1 t
where t.xsl is not null and t.xsl >= 0 and t.xsl <= 12
group by t.city;


-----------日期----------
select t.date_date,
       avg(t.xsl) avg_xsl,
       max(t.xsl) max_xsl,
       min(t.xsl) min_xsl,
       count(t.xsl) count_xsl,
       stddev(t.xsl) std_xsl
from t_tq_data_temp_xsl_20190404_1 t
where t.xsl is not null and t.xsl >= 0 and t.xsl <= 12
group by t.date_date
order by t.date_date;



-----------台区----------
select t.tg_no,
       avg(t.xsl) avg_xsl,
       max(t.xsl) max_xsl,
       min(t.xsl) min_xsl,
       count(t.xsl) count_xsl,
       stddev(t.xsl) std_xsl
from t_tq_data_temp_xsl_20190404_1 t
where t.xsl is not null and t.xsl >= 0 and t.xsl <= 12
group by t.tg_no
order by t.tg_no;



---------------hive抽取脚本，抽取导线长度
select 
  a.obj_id obj_id_zsb,
  a.sbbm sbbm_zsb,
  b.xlzcd,
  b.jkxlcd,
  b.dlxlcd,
  c.obj_id obj_id_dydx,
  c.sbmc,
  c.sbbm sbbm_dydx,
  c.qsgt,
  c.zzgt,
  c.ywdw,
  c.yxzt,
  c.dxcd,
  c.qslx,
  c.zzlx
from sg_data_sharing_warehouse.t_sb_zwyc_zsbyq a 
join sg_data_sharing_warehouse.t_sb_dysb_dyxl b on a.obj_id = b.ssbyq
join sg_data_sharing_warehouse.t_sb_dysb_dydx c on b.obj_id = c.ssxl
where a.sbbm 
  in('12M00000027123016',
  '12M00000040632839',
  '12M00000141176725',
  '12M00000138802680',
  '12M00000044294932',
  '12M00000044184316',
  '12M00000038393307',
  '12M00000042790667',
  '12M00000043904412',
  '12M00000149133652') 
and a.update_time=DATE_SUB(CURRENT_DATE(),1)
and b.update_time=DATE_SUB(CURRENT_DATE(),1)
and c.update_time=DATE_SUB(CURRENT_DATE(),1);



---------汇总熵值法建模数据-----------------
select 
  distinct t1.tg_no,
  t1.chg_date,
  t1.tg_cap,
  t1.tg_cons_cap,
  t1.tg_cons_jm_cap,
  t1.tg_cons_jm_cap/t1.tg_cons_cap percent1,
  t1.tg_cons_gy_cap,
  t1.tg_cons_gy_cap/t1.tg_cons_cap percent2,
  t1.tg_cons_sy_cap,
  t1.tg_cons_sy_cap/t1.tg_cons_cap percent3,
  t1.tg_cons_qt_cap,
  t1.tg_cons_qt_cap/t1.tg_cons_cap percent4,
  t1.tg_cons_ny_cap,
  t1.tg_cons_ny_cap/t1.tg_cons_cap percent5,
  t1.tg_v_cap01,
  t1.tg_v_cap01/t1.tg_cons_cap percent6,
  t1.tg_v_cap02,
  t1.tg_v_cap02/t1.tg_cons_cap percent7,
  t2.model_no,
  t1.tg_city_type,
  t1.scity,
  avg(t1.xsl) avg_xsl,
  stddev(t1.xsl) std_xsl,
  avg(t1.factor_avg) avg_factor,
  avg(t1.factor_max) max_factor,
  avg(t1.factor_min) min_factor,
  avg(t1.cur_max) avg_cur_max,
  avg(t1.cur_min) avg_cur_min,
  avg(t1.cur_avg) avg_cur_avg,
  avg(t1.fgcl) avg_fgcl,
  avg(t1.fhl) avg_fhl,
  avg(t1.avg_fzl) avg_fzl
from t_tq_data_base_20190411_1 t1 left join t_tq_base t2 on t1.tg_no = t2.tg_no
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null
group by t1.chg_date,
  t1.tg_cap,
  t1.tg_cons_cap,
  t1.tg_cons_jm_cap,
  t1.tg_cons_gy_cap,
  t1.tg_cons_sy_cap,
  t1.tg_cons_qt_cap,
  t1.tg_cons_ny_cap,
  t2.model_no,
  t1.tg_city_type,
  t1.tg_no,
  t1.scity,
  t1.tg_v_cap01,
  t1.tg_v_cap02;


  
-------------从hive导出后的数据到宽表------
--create table t_tq_data_base_20190411_1 as 
SELECT a1.tg_no,
       a1.data_date,
       a1.factor_avg,
       a1.factor_max,
       a1.factor_min,
       a1.cur_max,
       a1.cur_min,
       a1.cur_avg,
       a1.fgcl,
       a1.fhl,
       a1.avg_fzl,
       d1.xsl,
       case when d1.xsl >= 0 and d1.xsl <= 4 then 0
         when d1.xsl > 4 and d1.xsl <= 7 then 1
         when d1.xsl > 7 and d1.xsl <= 12 then 2
         else null end as xsl_type,
       d2.swgf_dl,
       a2.tg_cap,
       a2.chg_date,
       a2.tg_cons_cap,
       a2.tg_cons_jm_cap,
       a2.tg_cons_gy_cap,
       a2.tg_cons_sy_cap,
       a2.tg_cons_qt_cap,
	   a2.tg_cons_ny_cap,
       a2.tg_v_cap01,
       a2.tg_v_cap02,
       a2.tg_city_type,
	   a2.model_no,
       mm.date_date,
       mm.temperature1,
       mm.temperature2,
       (mm.temperature1+mm.temperature2)/2 avg_temp,
       mm.weather12,
       mm.weather24,
       mm.org_no,
       case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
         else null end as scity,
       mm.city as dcity
      FROM t_tq_index_all a1 
      left join t_tq_data d1 on a1.tg_no = d1.tg_no and to_date(a1.data_date,'YYYY-MM-DD') = d1.report_date
      left join t_gfdl_20190327 d2 on a1.tg_no = d2.tg_no and to_date(a1.data_date,'YYYY-MM-DD') = to_date(d2.date_time,'YYYY-MM-DD')
      left join t_tq_base a2 on a1.tg_no = a2.tg_no
      left join (
            select aa.tg_no,b.org_no,c.city,c.date_date,c.temperature1,c.temperature2,c.weather12,c.weather24
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm  on a1.tg_no = mm.tg_no where to_date(a1.data_date,'YYYY-MM-DD') = mm.date_date;


	 
---------------抽取全量指标--------
	   select t.tg_no,
       t.chg_date,
       t.tg_cap,
       t3.cons_num,
       t.tg_cap/t3.cons_num per_cap,
       t.tg_cons_cap,
       t.tg_cons_jm_cap,
       t.tg_cons_gy_cap,
       t.tg_cons_sy_cap,
       t.tg_cons_ny_cap,
       t.tg_cons_qt_cap,
	   t.tg_v_cap01,
       t.tg_v_cap02,
       t.model_no,
       t.tg_city_type,
       t2.avg_xsl,
       t2.std_xsl,
	   t2.avg_swgf_dl,
	   t2.std_swgf_dl,
       case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
         else null end as city
from T_TQ_BASE t 
      left join (select x.tg_no,
       avg(x.xsl) avg_xsl,
       stddev(x.xsl) std_xsl,
       gf.avg_swgf_dl,
       gf.std_swgf_dl
     from t_tq_data x
      left join (select d2.tg_no,avg(d2.swgf_dl) avg_swgf_dl,stddev(d2.swgf_dl) std_swgf_dl 
     from t_gfdl_20190327 d2 
     group by d2.tg_no) gf on x.tg_no = gf.tg_no
     where x.xsl is not null and x.xsl >= 0 and x.xsl <= 12
     group by x.tg_no,gf.avg_swgf_dl,gf.std_swgf_dl) t2 on t.tg_no = t2.tg_no
     left join (
            select distinct aa.tg_no,b.org_no
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm on t.tg_no = mm.tg_no
	  left join 
    ( select cc.tg_no,count(cc.cons_name) cons_num from t_tq_cons cc group by cc.tg_no having count(cc.cons_name) > 0) t3 on t.tg_no = t3.tg_no;

  
  
  -----------线损率---
select t.tg_no,
       count(1) xsl_total,
       sum(case when t.xsl is null then 1 end) xsl_null,
       sum(case when t.xsl < 0 then 1 end) xsl_0,
       sum(case when t.xsl >= 0 and t.xsl <= 4 then 1 end) xsl_0_4,
       sum(case when t.xsl > 4 and t.xsl <= 7 then 1 end) xsl_4_7,
       sum(case when t.xsl > 7 and t.xsl <= 12 then 1 end) xsl_7_12,
       sum(case when t.xsl > 12 then 1 end) xsl_12,
       sum(case when t.xsl >= 0 and t.xsl <= 12 then t.xsl end)/sum(case when t.xsl >= 0 and t.xsl <= 12 then 1 end) avg_xsl，
     case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from T_TQ_DATA t left join 
(select x.tg_no,
             avg(x.xsl) avg_xsl,
             stddev(x.xsl) std_xsl
     from t_tq_data x
     where x.xsl is not null and x.xsl >= 0 and x.xsl <= 12
     group by x.tg_no) t2 on t.tg_no = t2.tg_no
     left join (
            select distinct aa.tg_no,b.org_no
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm on t.tg_no = mm.tg_no
group by t.tg_no,mm.org_no;
  
------------------线损率随时间-----
select t.report_date,
       avg(t.xsl) avg_xsl,
       stddev(t.xsl) std_xsl
from T_TQ_DATA t
where t.xsl >= 0 and t.xsl <= 12 and t.xsl is not null
group by t.report_date;




-------------------供电半径--线损率---------
select t1.tg_no,
       t3.lengt,
       avg(t1.xsl) avg_xsl,
       stddev(t1.xsl) std_xsl
from T_TQ_DATA t1 
     left join t_tq_tqlength t3 on t1.tg_no = t3.tg_no
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null and t3.lengt is not null and t3.lengt <= 2000
group by t1.tg_no,t3.lengt;




-------------线路相关指标--线损率-------
select t1.tg_no,
       avg(t1.xsl) avg_xsl,
       stddev(t1.xsl) std_xsl,
       t2.total,
       t2.jmj_1,
       t2.jmj_2,
       t2.jmj_3,
       t2.jmj_4,
       t2.jmj_5,
       t2.jmj_null,
       t2.cl_01,
       t2.cl_02,
       t2.cl_null,
       t2.datatype_dld,
       t2.datatype_dx,
       t2.datatype_null,
       t2.cd_total,
       case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from T_TQ_DATA t1 left join T_TQ_DYXL_PROP t2 on t1.tg_no = t2.tg_no
     left join (
            select distinct aa.tg_no,b.org_no
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm on t1.tg_no = mm.tg_no
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null and t2.total is not null
group by t1.tg_no,t2.total,t2.jmj_1,t2.jmj_2,t2.jmj_3,t2.jmj_4,t2.jmj_5,t2.jmj_null,t2.cl_01,t2.cl_02,t2.cl_null,t2.datatype_dld,t2.datatype_dx,t2.datatype_null,t2.cd_total,mm.org_no;







-------------异常电量（4月）-线损率（-3月）--------------
select t1.tg_no,
       avg(t4.total) avg_total,
       avg(t4.zc_total) avg_zc_total,
       avg(t4.zcl) avg_zcl,
       avg(t1.xsl) avg_xsl,
       stddev(t1.xsl) std_xsl
from T_TQ_DATA t1 
     left join T_TQ_YCDL_20190410_1 t4 on t1.tg_no = t4.tg_no
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null and avg(t4.total) is not null
group by t1.tg_no,t4.tg_no;



---稳定台区的运行年限-线损率-----------------------

select t1.report_date,
       avg(t1.xsl) avg_xsl,
       stddev(t1.xsl) std_xsl
from T_TQ_DATA t1
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null and t1.tg_no in ('2037701989','0120661989','2037704876','0120190068','2037701501')
group by t1.report_date;



-----------------------平均温度分组统计数据-------------------------
select ceil(tt.avg_temp), avg(tt.avg_xsl) avg_xsl, stddev(tt.avg_xsl) std_xsl 
from (
select t1.report_date,
       avg(t1.xsl) avg_xsl,
       stddev(t1.xsl) std_xsl,
       mm.avg_temp
from T_TQ_DATA t1
left join 
     (select c.date_date,avg((c.temperature1+c.temperature2)/2) avg_temp
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
            where aa.tg_no in ('2037701989','0120661989','2037704876','0120190068','2037701501')
            group by c.date_date
      )  mm  on mm.date_date = t1.report_date
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null and t1.tg_no in ('2037701989','0120661989','2037704876','0120190068','2037701501')
and mm.avg_temp is not null
group by t1.report_date,mm.avg_temp) tt
group by ceil(tt.avg_temp);


-------------功率相关指标数据-------
select t1.tg_no,
       t2.report_date,
       t1.fgcl,
       t1.fhl,
       t1.avg_fzl,
       t2.xsl,
       '合肥市' city
from t_tq_index_power_all t1 left join T_TQ_DATA t2 on t1.tg_no = t2.tg_no and to_date(t1.data_date,'YYYY-MM-DD') = t2.report_date
left join (
            select distinct aa.tg_no,b.org_no
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm on t1.tg_no = mm.tg_no
where t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12 and substr(mm.org_no,0,5) = '34401' and t1.fgcl != 0;




select count(1) from (
select t1.tg_no,
       t2.report_date,
       t1.fgcl,
       t1.fhl,
       t1.avg_fzl,
       t2.xsl,
       '合肥市' city
from t_tq_index_power_all t1 left join T_TQ_DATA t2 on t1.tg_no = t2.tg_no and to_date(t1.data_date,'YYYY-MM-DD') = t2.report_date
left join (
            select distinct aa.tg_no,b.org_no
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm on t1.tg_no = mm.tg_no
where t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12 and t1.fgcl != 0
and substr(mm.org_no,0,5) = '34401'
and t2.report_date > to_date('2019/2/20','YYYY-MM-DD') and t2.report_date <= to_date('2019/4/1','YYYY-MM-DD')
) zz ;

select substr(t.report_date,4,6) from T_TQ_DATA t;



select count(1) from (
select t1.tg_no,
       substr(t1.data_date,0,6) data_month,
       avg(t1.fgcl) avg_fgcl,
       stddev(t1.fgcl) std_fgcl,
       avg(t1.fhl) avg_fhl,
       stddev(t1.fhl) std_fhl,
       avg(t1.avg_fzl) avg_fzl,
       stddev(t1.avg_fzl) std_fzl,
       avg(t2.xsl) avg_xsl,
       stddev(t2.xsl) std_xsl,
       case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from t_tq_index_power_all t1 left join T_TQ_DATA t2 on t1.tg_no = t2.tg_no and to_date(t1.data_date,'YYYY-MM-DD') = t2.report_date
left join (
            select distinct aa.tg_no,b.org_no
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm on t1.tg_no = mm.tg_no
where t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12 and t1.fgcl != 0
group by t1.tg_no,mm.org_no,substr(t1.data_date,0,6)
order by t1.tg_no,substr(t1.data_date,0,6)
) zz ;

------------------扩展温度区间---------------------
select mm.hcity,
       mm.data_date,
       avg(mm.avg_temp) avg_temp,
       avg(t1.xsl) avg_xsl,
       stddev(t1.xsl) std_xsl
from (select aa.tg_no,c.data_date,c.hcity,c.avg_temp
      from(
             select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
             union all
             select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
           ) aa
      join t_org b on aa.org_no = b.org_no
      join 
      (select case when t.city = 'hefei' then '合肥' 
            when t.city = 'chuzhou' then '滁州'
            when t.city = 'bozhou' then '亳州'
            when t.city = 'huangshan' then '黄山'
            when t.city = 'huangshanfengjingqu' then '黄山'
            else null end hcity,
            to_date(t.data,'YYYY"年"MM"月"DD"日"') data_date,
            (t.tempmax + t.tempmin)/2 avg_temp
       from T_TQ_WEATHER02 t
      ) c on b.jc = c.hcity) mm 
      left join T_TQ_DATA t1 on t1.report_date = mm.data_date and t1.tg_no = mm.tg_no
where t1.xsl is not null and t1.xsl >= 0 and t1.xsl <= 12 
group by mm.hcity,mm.data_date
order by mm.hcity,mm.data_date
union all
select substr(tt.scity,0,2) hcity, 
       tt.date_date data_date,
       avg(tt.avg_temp) avg_temp,
       avg(tt.xsl) avg_xsl,
       stddev(tt.xsl) std_xsl
from t_tq_data_base_20190411_1 tt
where tt.xsl is not null and tt.xsl >= 0 and tt.xsl <= 12 and tt.date_date >= to_date('20190101','YYYY-MM-DD-')
group by tt.scity,tt.date_date
order by tt.scity,tt.date_date


------------用户电量--距离（计算供电半径）-------------------
create table t_tq_daily_box_sum as
select t.id,t.asset_no,sum(t.avg_cal) total_sum, count(t.avg_cal) total_count from t_tq_daily_box t group by t.id,t.asset_no;


select t1.tg_id，t1.tg_no,t1.byqsbid,t1.id jlx_id,t1.asset_no jlx_assetNo,t1.length*1000,t2.total_sum,t2.total_count
from cacher01.g_tg t0 
left join t_tq_length_box_tg t1 on t0.tg_id = t1.tg_id
left join t_tq_daily_box_sum t2 on t1.id = t2.id
where substr(t0.org_no,0,5) in ('34401') and t1.length<=2 and t1.length is not null and t2.total_sum is not null order by t1.tg_id;




----电量--台区--天--线损率
select count(1) from (
select t.tg_no,
       t.caldt data_date,
       t.cal_sum,
       t.cal_count,
       t.cal_sum/t.cal_count cal_avg,
       t2.xsl,
       case when substr(t3.org_no,0,5) = '34401' then '合肥市'
         when substr(t3.org_no,0,5) = '34412' then '黄山市'
         when substr(t3.org_no,0,5) = '34413' then '滁州市'
         when substr(t3.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from T_TQ_CAL_20190422 t join T_TQ_DATA t2 on t.tg_no = t2.tg_no and to_date(t.caldt,'YYYY-MM-DD') = t2.report_date
left join t_tq_base t3 on t.tg_no = t3.tg_no
where t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12
and substr(t3.org_no,0,5) = '34412'
and t2.report_date > to_date('2018/6/1','YYYY-MM-DD') and t2.report_date <= to_date('2018/9/1','YYYY-MM-DD')
) mm;





----电量--台区--月--线损率


select t.tg_no,
       substr(t.caldt,0,7) data_month,
       sum(t.cal_sum) cal_sum,
       sum(t.cal_count) cal_count,
       sum(t.cal_sum)/sum(t.cal_count) cal_avg,
       avg(t2.xsl) xsl_avg,
       stddev(t2.xsl) xsl_std,
       case when substr(t3.org_no,0,5) = '34401' then '合肥市'
         when substr(t3.org_no,0,5) = '34412' then '黄山市'
         when substr(t3.org_no,0,5) = '34413' then '滁州市'
         when substr(t3.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from T_TQ_CAL_20190422 t join T_TQ_DATA t2 on t.tg_no = t2.tg_no and to_date(t.caldt,'YYYY-MM-DD') = t2.report_date
left join t_tq_base t3 on t.tg_no = t3.tg_no
where t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12
group by t.tg_no,t3.org_no,substr(t.caldt,0,7)
order by t.tg_no,substr(t.caldt,0,7)


----电量--台区--线损率
select t.tg_no,
       sum(t.cal_sum) cal_sum,
       sum(t.cal_count) cal_count,
       sum(t.cal_sum)/sum(t.cal_count) cal_avg,
       avg(t2.xsl) xsl_avg,
       stddev(t2.xsl) xsl_std,
       case when substr(t3.org_no,0,5) = '34401' then '合肥市'
         when substr(t3.org_no,0,5) = '34412' then '黄山市'
         when substr(t3.org_no,0,5) = '34413' then '滁州市'
         when substr(t3.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from T_TQ_CAL_20190422 t join T_TQ_DATA t2 on t.tg_no = t2.tg_no and to_date(t.caldt,'YYYY-MM-DD') = t2.report_date
left join t_tq_base t3 on t.tg_no = t3.tg_no
where t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12
group by t.tg_no,t3.org_no
order by t.tg_no


--------最大/最小温度-线损率-----------
select case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
     else null end as city,
	   t.report_date,
	   avg(t.xsl) avg_xsl,
	   avg(mm.temperature1) avg_max,
	   avg(mm.temperature2) avg_min
from (select aa.tg_no,c.date_date,c.temperature1,c.temperature2,(c.temperature1+c.temperature2)/2 avg_temp,aa.org_no
      from(
        select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
        union all
        select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
      ) aa
      join t_org b on aa.org_no = b.org_no
      join t_weather c on b.jc = c.city) mm 
      left join T_TQ_DATA t on t.report_date = mm.date_date and t.tg_no = mm.tg_no
	group by t.report_date,mm.org_no;



select count(1) from 
(select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201805 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201806 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201807 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201808 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201809 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201810 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201811 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201812 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201901 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201902 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
UNION ALL
select t.tg_no,t.data_date,avg(t.data) power_avg
from result_power_201903 t
where t.phase_flag = 1 and t.data is not null and t.data > 0
group by t.tg_no,t.data_date
) mm;




----------------平均功率-------------
select count(1) from (

select t2.tg_no,
       t2.report_date,
       t1.power_avg,
       t2.xsl,
       case when substr(t3.org_no,0,5) = '34401' then '合肥市'
         when substr(t3.org_no,0,5) = '34412' then '黄山市'
         when substr(t3.org_no,0,5) = '34413' then '滁州市'
         when substr(t3.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from t_tq_power t1 
left join t_tq_data t2 on t1.tg_no = t2.tg_no and to_date(t1.data_date,'YYYY-MM-DD') = t2.report_date
left join t_tq_base t3 on t1.tg_no = t3.tg_no
where t2.report_date >= to_date('2018/5/1','YYYY-MM-DD')
and t2.xsl is not null and t2.xsl >= 0 and t2.xsl <= 12 
and substr(t3.org_no,0,5) = '34417'
and t2.report_date > to_date('2019/2/20','YYYY-MM-DD') and t2.report_date <= to_date('2019/4/1','YYYY-MM-DD')

) mm;



-------------建立评价模型所需数据--固定指标---------------
select t1.tg_no,
       t1.chg_date,
       t1.tg_cap,
       t3.cons_num,
       case when t3.cons_num is not null and t3.cons_num != 0 then t1.tg_cap/t3.cons_num else null end per_cap,
       case when t1.tg_cons_cap is not null and t1.tg_cons_cap != 0 then t1.tg_cons_jm_cap/t1.tg_cons_cap else null end percent_jm,
       case when t1.tg_cons_cap is not null and t1.tg_cons_cap != 0 then t1.tg_cons_gy_cap/t1.tg_cons_cap else null end percent_gy,
       case when t1.tg_cons_cap is not null and t1.tg_cons_cap != 0 then t1.tg_cons_sy_cap/t1.tg_cons_cap else null end percent_sy,
       case when t1.tg_cons_cap is not null and t1.tg_cons_cap != 0 then t1.tg_cons_ny_cap/t1.tg_cons_cap else null end percent_ny,
       case when t1.tg_cons_cap is not null and t1.tg_cons_cap != 0 then t1.tg_cons_qt_cap/t1.tg_cons_cap else null end percent_qt,
       t2.cl_01,
       t2.cl_02,
       t2.cl_null,
       t2.datatype_dld,
       t2.datatype_dx,
       t2.datatype_null,
       t2.cd_total,
       t4.xsl_avg
from t_tq_base t1 
left join t_tq_dyxl_prop t2 on t1.tg_no = t2.tg_no
left join
    (select cc.tg_no,count(cc.cons_name) cons_num 
    from t_tq_cons cc 
    group by cc.tg_no 
    having count(cc.cons_name) > 0) t3 on t1.tg_no = t3.tg_no
left join
    (select aa.tg_no,avg(aa.xsl) xsl_avg 
    from t_tq_data aa 
    where aa.xsl > 0 and aa.xsl <= 12 and aa.xsl is not null and aa.report_date >= to_date('2018/5/1','YYYY-MM-DD')
    group by aa.tg_no ) t4 on t1.tg_no = t4.tg_no
where substr(t1.org_no,0,5) in ('34401','34412','34413','34417');








------------建立预测模型所需数据----------------
create table t_tq_data_temp_20190428_1 as
select t2.tg_no,
       t2.report_date,
       t2.xsl,
       t1.cal_sum,
       t1.cal_count,
       t3.power_avg,
       t4.fgcl,
       t4.fhl,
       case when t5.tg_cap is not null and t5.tg_cap != 0 then t4.avg_fzl/t5.tg_cap else null end fzl,
       mm.temperature1,
       mm.temperature2,
       mm.avg_temp,
       case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
     else null end as city
from t_tq_cal_20190422 t1 
inner join t_tq_data t2 on t1.tg_no = t2.tg_no and to_date(t1.caldt,'YYYY-MM-DD') = t2.report_date
inner join t_tq_power t3 on t1.tg_no = t3.tg_no and to_date(t1.caldt,'YYYY-MM-DD') = to_date(t3.data_date,'YYYY-MM-DD')
inner join t_tq_index_power_all t4 on t1.tg_no = t4.tg_no and to_date(t1.caldt,'YYYY-MM-DD') = to_date(t4.data_date,'YYYY-MM-DD')
inner join (select aa.tg_no,c.date_date,c.temperature1,c.temperature2,(c.temperature1+c.temperature2)/2 avg_temp,aa.org_no
      from(
        select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
        union all
        select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
      ) aa
      join t_org b on aa.org_no = b.org_no
      join t_weather c on b.jc = c.city) mm on t2.report_date = mm.date_date and t1.tg_no = mm.tg_no
left join t_tq_base t5 on t1.tg_no = t5.tg_no
where t2.xsl > 0 and t2.xsl <= 12 and t2.xsl is not null and t2.report_date >= to_date('2018/5/1','YYYY-MM-DD')
and t2.tg_no is not null and t2.report_date is not null and t2.xsl is not null and t1.cal_sum is not null and t1.cal_count is not null 
and t3.power_avg is not null and t4.fgcl is not null and t4.fhl is not null and t4.avg_fzl is not null 
and mm.temperature1 is not null and mm.temperature2 is not null and mm.avg_temp is not null
and t5.tg_cap is not null and t5.tg_cap != 0


------抽取数据供计算供电密度------------------------
select count(1) from (
select t1.tg_no,t4.meter_id,t1.length,t4.avg_cal,aa.avg_xsl from T_TQ_ZSPDBYQ_BOX_TOTAL t1 
join cacher01.d_meas_box t2 on t1.assetno = t2.asset_no
left join cacher01.c_container_dev t3 on t2.id = t3.container_id
join t_tq_cal_chuzhou_20190515 t4 on t3.equip_id = t4.meter_id
left join (select t5.tg_no,avg(t5.xsl) avg_xsl from t_tq_data t5 
where t5.report_date >= to_date('2018/5/1','YYYY-MM-DD')
and t5.xsl is not null and t5.xsl >= 0 and t5.xsl <= 12 
group by t5.tg_no ) aa
on t1.tg_no = aa.tg_no 
) aaa


-------------原始数据至建模数据量化处理---------------
create table t_tq_data_20190516 as
select t1.tg_no,
       t1.chg_date,
       t2.cd_total,
       case when t3.cons_num is not null and t3.cons_num != 0 then t1.tg_cap/t3.cons_num else null end per_cap,
       case when t1.model_no like '%SCB%' then 1
         when t1.model_no like '%S9%' or t1.model_no like '%S10%' or t1.model_no like '%S11%' or t1.model_no like '%S13%' or t1.model_no like '%SB%' or t1.model_no like '%SZ%' then 0
         else 2 end byqxh,
       t1.tg_cap,
       case when t1.tg_city_type = '城市' then 0
         when t1.tg_city_type = '混合' then 1
         when t1.tg_city_type = '农村' then 2
         when t1.tg_city_type = '特殊边远山区' then 3
         else 4 end city_type,
       case when substr(t1.org_no,0,5) = '34401' then 0
         when substr(t1.org_no,0,5) = '34412' then 2
         when substr(t1.org_no,0,5) = '34413' then 1
         when substr(t1.org_no,0,5) = '34417' then 3
         else 4 end as city,
       case when t2.cl_01 >= t2.cl_02 and t2.cl_01 > t2.cl_null then 0
         when t2.cl_02 > t2.cl_01 and t2.cl_02 > t2.cl_null then 1
         else 2 end dxcz,
       case when t2.datatype_dld >= t2.datatype_dx and t2.datatype_dld > t2.datatype_null then 1
         when t2.datatype_dx >= t2.datatype_dld and t2.datatype_dx > t2.datatype_null then 0
         else 2 end dxtype,
       case when t1.tg_cons_jm_cap > t1.tg_cons_gy_cap and t1.tg_cons_jm_cap > t1.tg_cons_sy_cap and t1.tg_cons_jm_cap > t1.tg_cons_ny_cap and t1.tg_cons_jm_cap > t1.tg_cons_qt_cap then 0 
         when t1.tg_cons_gy_cap > t1.tg_cons_jm_cap and t1.tg_cons_gy_cap > t1.tg_cons_sy_cap and t1.tg_cons_gy_cap > t1.tg_cons_ny_cap and t1.tg_cons_gy_cap > t1.tg_cons_qt_cap then 1
         when t1.tg_cons_sy_cap > t1.tg_cons_jm_cap and t1.tg_cons_sy_cap > t1.tg_cons_gy_cap and t1.tg_cons_sy_cap > t1.tg_cons_ny_cap and t1.tg_cons_sy_cap > t1.tg_cons_qt_cap then 2
         when t1.tg_cons_ny_cap > t1.tg_cons_gy_cap and t1.tg_cons_ny_cap > t1.tg_cons_sy_cap and t1.tg_cons_ny_cap > t1.tg_cons_jm_cap and t1.tg_cons_ny_cap > t1.tg_cons_qt_cap then 3
         else 4 end cons_type
from t_tq_base t1 
left join t_tq_dyxl_prop t2 on t1.tg_no = t2.tg_no
left join
    (select cc.tg_no,count(cc.cons_name) cons_num 
    from t_tq_cons cc 
    group by cc.tg_no 
    having count(cc.cons_name) > 0) t3 on t1.tg_no = t3.tg_no
where t1.chg_date is not null and t1.tg_cap is not null and t1.tg_city_type is not null 
and t1.org_no is not null and t1.model_no is not null and t2.cd_total is not null;

------------按照春秋、夏冬分类统计数据----------------------
create table t_tq_data_index_20190516_2 as
select t1.tg_no,
       avg(t2.fhl) fhl,
       avg(t1.cal_sum) cal,
       avg(t2.avg_fzl) avg_fzl,
       avg(t2.fgcl) fgcl,
       case when avg(t3.swgf_dl) > 0 then 0
         else 1 end swgf,
       avg(t4.xsl) xsl
from t_tq_cal_all t1
left join t_tq_index_power_all t2 on t1.tg_no = t2.tg_no and to_date(t1.caldt,'YYYY-MM-DD') = to_date(t2.data_date,'YYYY-MM-DD')
left join t_gfdl_20190327 t3 on t1.tg_no = t3.tg_no and to_date(t1.caldt,'YYYY-MM-DD') = to_date(t3.date_time,'YYYY-MM-DD')
left join t_tq_data t4 on t4.tg_no = t1.tg_no and t4.report_date = to_date(t1.caldt,'YYYY-MM-DD')
where t4.xsl is not null and t4.xsl >= 0 and t4.xsl <= 12 
/*and substr(t2.data_date,5,2) in ('03','04','05','09','10','11')*/
and substr(t2.data_date,5,2) in ('06','07','08','12','01','02')
group by t1.tg_no;



select t1.tg_no,
       t1.chg_date,
       t1.cd_total,
       t1.per_cap,
       t1.byqxh,
       t1.tg_cap,
       t1.city_type,
       t1.city,
       t1.dxcz,
       t1.dxtype,
       t1.cons_type,
       t2.fhl,
       t2.cal,
       t2.avg_fzl,
       t2.fgcl,
       t2.swgf,
       t2.xsl
from t_tq_data_20190516 t1 
join t_tq_data_index_20190516_1 t2 on t1.tg_no = t2.tg_no;


----------计算各指标占比数据----------------
select 
  distinct t1.tg_no,
  t1.chg_date,
  t1.tg_cap,
  t1.tg_cons_cap,
  t1.tg_cons_jm_cap,
  t1.tg_cons_jm_cap/t1.tg_cons_cap percent1,
  t1.tg_cons_gy_cap,
  t1.tg_cons_gy_cap/t1.tg_cons_cap percent2,
  t1.tg_cons_sy_cap,
  t1.tg_cons_sy_cap/t1.tg_cons_cap percent3,
  t1.tg_cons_qt_cap,
  t1.tg_cons_qt_cap/t1.tg_cons_cap percent4,
  (t1.tg_cons_jm_cap + t1.tg_cons_gy_cap +　t1.tg_cons_sy_cap + t1.tg_cons_qt_cap)/t1.tg_cons_cap percent5,
  t1.tg_v_cap01,
  t1.tg_v_cap01/t1.tg_cons_cap percent6,
  t1.tg_v_cap02,
  t1.tg_v_cap02/t1.tg_cons_cap percent7,
  t2.model_no,
  t1.tg_city_type,
  t1.city,
  avg(t1.xsl) avg_xsl,
  stddev(t1.xsl) std_xsl,
  avg(t1.factor_avg) avg_factor,
  avg(t1.cur_max) avg_cur_max,
  avg(t1.cur_min) avg_cur_min,
  avg(t1.cur_avg) avg_cur_avg,
  avg(t1.fgcl) avg_fgcl,
  avg(t1.fhl) avg_fhl,
  avg(t1.avg_fzl) avg_fzl
from t_tq_data_base_20190403_2 t1 left join t_tq_base t2 on t1.tg_no = t2.tg_no
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null
group by t1.chg_date,
  t1.tg_cap,
  t1.tg_cons_cap,
  t1.tg_cons_jm_cap,
  t1.tg_cons_gy_cap,
  t1.tg_cons_sy_cap,
  t1.tg_cons_qt_cap,
  t2.model_no,
  t1.tg_city_type,
  t1.tg_no,
  t1.city,
  t1.tg_v_cap01,
  t1.tg_v_cap02;
  
  
  
  
  
  --------全量指标----------
  select 
  count(distinct t1.tg_no)
from t_tq_data_base_20190403_2 t1 left join t_tq_base t2 on t1.tg_no = t2.tg_no
where t1.xsl >= 0 and t1.xsl <= 12 and t1.xsl is not null
group by t1.chg_date,
  t1.tg_cap,
  t1.tg_cons_cap,
  t1.tg_cons_jm_cap,
  t1.tg_cons_gy_cap,
  t1.tg_cons_sy_cap,
  t1.tg_cons_qt_cap,
  t2.model_no,
  t1.tg_city_type,
  t1.tg_no,
  t1.swgf_dl,
  t1.city;
  
  
  
----------------汇总全指标--------------
create table t_tq_data_base_20190409_1 as 
SELECT a1.tg_no,
       a1.data_date,
       a1.factor_avg,
       a1.factor_max,
       a1.factor_min,
       a1.cur_max,
       a1.cur_min,
       a1.cur_avg,
       a1.fgcl,
       a1.fhl,
       a1.avg_fzl,
       d1.xsl,
       case when d1.xsl >= 0 and d1.xsl <= 4 then 0
         when d1.xsl > 4 and d1.xsl <= 7 then 1
         when d1.xsl > 7 and d1.xsl <= 12 then 2
         else null end as xsl_type,
       d2.swgf_dl,
       a2.tg_cap,
       a2.chg_date,
       a2.tg_cons_cap,
       a2.tg_cons_jm_cap,
       a2.tg_cons_gy_cap,
       a2.tg_cons_sy_cap,
       a2.tg_cons_qt_cap,
       a2.tg_v_cap01,
       a2.tg_v_cap02,
       a2.tg_city_type,
       mm.date_date,
       mm.temperature1,
       mm.temperature2,
       (mm.temperature1+mm.temperature2)/2 avg_temp,
       mm.weather12,
       mm.weather24,
       mm.org_no,
       case when substr(mm.org_no,0,5) = '34401' then '合肥市'
         when substr(mm.org_no,0,5) = '34412' then '黄山市'
         when substr(mm.org_no,0,5) = '34413' then '滁州市'
         when substr(mm.org_no,0,5) = '34417' then '亳州市'
         else null end as scity,
       mm.city as dcity
      FROM t_tq_index_all a1 
      left join t_tq_data d1 on a1.tg_no = d1.tg_no and to_date(a1.data_date,'YYYY-MM-DD') = d1.report_date
      left join t_gfdl_20190327 d2 on a1.tg_no = d2.tg_no and to_date(a1.data_date,'YYYY-MM-DD') = to_date(d2.date_time,'YYYY-MM-DD')
      left join t_tq_base a2 on a1.tg_no = a2.tg_no
      left join (
            select aa.tg_no,b.org_no,c.city,c.date_date,c.temperature1,c.temperature2,c.weather12,c.weather24
            from(
              select a.tg_no,a.org_no from t_tq_base a where length(a.org_no) <= 7
              union all
              select b.tg_no,substr(b.org_no,0,7) from t_tq_base b where length(b.org_no) > 7
            ) aa
            join t_org b on aa.org_no = b.org_no
            join t_weather c on b.jc = c.city
      )  mm  on a1.tg_no = mm.tg_no where to_date(a1.data_date,'YYYY-MM-DD') = mm.date_date;




