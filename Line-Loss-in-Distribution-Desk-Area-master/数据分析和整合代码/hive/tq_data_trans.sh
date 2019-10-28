#!/bin/bash


hive -e "

use sjzl;

create table data_factor_$1 as 
select a1.tg_id, a1.tg_no, a.seq,a.data_id,a.data_date,a.org_no,a.col_addr,a.asset_no,a.meter_id,a.phase_flag,a.c1,a.c2,a.c3,a.c4,a.c5,a.c6,a.c7,a.c8,a.c9,a.c10,a.c11,a.c12,a.c13,a.c14,a.c15,a.c16,a.c17,a.c18,a.c19,a.c20,a.c21,a.c22,a.c23,a.c24,a.c25,a.c26,a.c27,a.c28,a.c29,a.c30,a.c31,a.c32,a.c33,a.c34,a.c35,a.c36,a.c37,a.c38,a.c39,a.c40,a.c41,a.c42,a.c43,a.c44,a.c45,a.c46,a.c47,a.c48,a.c49,a.c50,a.c51,a.c52,a.c53,a.c54,a.c55,a.c56,a.c57,a.c58,a.c59,a.c60,a.c61,a.c62,a.c63,a.c64,a.c65,a.c66,a.c67,a.c68,a.c69,a.c70,a.c71,a.c72,a.c73,a.c74,a.c75,a.c76,a.c77,a.c78,a.c79,a.c80,a.c81,a.c82,a.c83,a.c84,a.c85,a.c86,a.c87,a.c88,a.c89,a.c90,a.c91,a.c92,a.c93,a.c94,a.c95,a.c96,a.record_time,a.update_time
from root.dn_g_tg a1 left join root.dn_c_mp a2 on a1.tg_id = a2.tg_id and a2.type_code = '02' 
left join root.dn_c_meter_mp_rela a3 on a2.mp_id = a3.mp_id
left join sjzl.r_data_mp a4 on a3.meter_id = a4.meter_id
left join sg_data_sharing_warehouse.e_mp_factor_curve a on a4.id=a.data_id 
where substr(a1.org_no,0,5) in ('34401','34412','34413','34417') 
and a.data_date>='$101' and a.data_date<'$201'
and a1.update_time=DATE_SUB(CURRENT_DATE(),3) 
and a2.update_time=DATE_SUB(CURRENT_DATE(),3) 
and a3.update_time=DATE_SUB(CURRENT_DATE(),3);

create table result_factor_curve_$1 as 
select tg_id, tg_no, data_id,  data_date , phase_flag ,1 as datetime,  c1 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,2 as datetime,  c2 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,3 as datetime,  c3 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,4 as datetime,  c4 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,5 as datetime,  c5 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,6 as datetime,  c6 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,7 as datetime,  c7 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,8 as datetime,  c8 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,9 as datetime,  c9 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,10 as datetime,  c10 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,11 as datetime,  c11 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,12 as datetime,  c12 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,13 as datetime,  c13 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,14 as datetime,  c14 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,15 as datetime,  c15 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,16 as datetime,  c16 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,17 as datetime,  c17 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,18 as datetime,  c18 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,19 as datetime,  c19 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,20 as datetime,  c20 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,21 as datetime,  c21 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,22 as datetime,  c22 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,23 as datetime,  c23 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,24 as datetime,  c24 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,25 as datetime,  c25 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,26 as datetime,  c26 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,27 as datetime,  c27 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,28 as datetime,  c28 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,29 as datetime,  c29 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,30 as datetime,  c30 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,31 as datetime,  c31 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,32 as datetime,  c32 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,33 as datetime,  c33 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,34 as datetime,  c34 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,35 as datetime,  c35 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,36 as datetime,  c36 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,37 as datetime,  c37 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,38 as datetime,  c38 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,39 as datetime,  c39 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,40 as datetime,  c40 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,41 as datetime,  c41 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,42 as datetime,  c42 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,43 as datetime,  c43 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,44 as datetime,  c44 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,45 as datetime,  c45 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,46 as datetime,  c46 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,47 as datetime,  c47 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,48 as datetime,  c48 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,49 as datetime,  c49 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,50 as datetime,  c50 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,51 as datetime,  c51 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,52 as datetime,  c52 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,53 as datetime,  c53 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,54 as datetime,  c54 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,55 as datetime,  c55 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,56 as datetime,  c56 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,57 as datetime,  c57 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,58 as datetime,  c58 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,59 as datetime,  c59 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,60 as datetime,  c60 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,61 as datetime,  c61 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,62 as datetime,  c62 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,63 as datetime,  c63 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,64 as datetime,  c64 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,65 as datetime,  c65 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,66 as datetime,  c66 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,67 as datetime,  c67 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,68 as datetime,  c68 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,69 as datetime,  c69 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,70 as datetime,  c70 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,71 as datetime,  c71 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,72 as datetime,  c72 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,73 as datetime,  c73 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,74 as datetime,  c74 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,75 as datetime,  c75 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,76 as datetime,  c76 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,77 as datetime,  c77 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,78 as datetime,  c78 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,79 as datetime,  c79 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,80 as datetime,  c80 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,81 as datetime,  c81 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,82 as datetime,  c82 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,83 as datetime,  c83 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,84 as datetime,  c84 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,85 as datetime,  c85 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,86 as datetime,  c86 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,87 as datetime,  c87 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,88 as datetime,  c88 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,89 as datetime,  c89 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,90 as datetime,  c90 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,91 as datetime,  c91 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,92 as datetime,  c92 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,93 as datetime,  c93 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,94 as datetime,  c94 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,95 as datetime,  c95 as data from data_factor_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,96 as datetime,  c96 as data from data_factor_$1;


create table data_cur_$1 as 
select a1.tg_id, a1.tg_no, a.seq ,a.data_id ,a.area_code ,a.data_date ,a.ct ,a.org_no ,a.col_addr ,a.asset_no ,a.meter_id ,a.phase_flag ,a.i1 ,a.i2 ,a.i3 ,a.i4 ,a.i5 ,a.i6 ,a.i7 ,a.i8,a.i9 ,a.i10 ,a.i11 ,a.i12 ,a.i13 ,a.i14 ,a.i15 ,a.i16 ,a.i17 ,a.i18 ,a.i19 ,a.i20 ,a.i21 ,a.i22 ,a.i23 ,a.i24 ,a.i25 ,a.i26 ,a.i27 ,a.i28 ,a.i29 ,a.i30 ,a.i31 ,a.i32 ,a.i33 ,a.i34 ,a.i35 ,a.i36 ,a.i37 ,a.i38 ,a.i39 ,a.i40 ,a.i41 ,a.i42 ,a.i43 ,a.i44 ,a.i45 ,a.i46 ,a.i47 ,a.i48 ,a.i49 ,a.i50 ,a.i51 ,a.i52 ,a.i53 ,a.i54 ,a.i55 ,a.i56 ,a.i57 ,a.i58 ,a.i59 ,a.i60 ,a.i61 ,a.i62 ,a.i63 ,a.i64 ,a.i65 ,a.i66 ,a.i67 ,a.i68 ,a.i69 ,a.i70 ,a.i71 ,a.i72 ,a.i73 ,a.i74 ,a.i75 ,a.i76 ,a.i77 ,a.i78 ,a.i79 ,a.i80 ,a.i81 ,a.i82 ,a.i83 ,a.i84 ,a.i85 ,a.i86 ,a.i87 ,a.i88 ,a.i89 ,a.i90 ,a.i91 ,a.i92 ,a.i93 ,a.i94 ,a.i95 ,a.i96 ,a.record_time ,a.update_time
from root.dn_g_tg a1 left join root.dn_c_mp a2 on a1.tg_id = a2.tg_id and a2.type_code = '02'
left join root.dn_c_meter_mp_rela a3 on a2.mp_id = a3.mp_id
left join sjzl.r_data_mp a4 on a3.meter_id = a4.meter_id
left join sg_data_sharing_warehouse.e_mp_cur_curve a on a4.id=a.data_id 
where substr(a1.org_no,0,5) in ('34401','34412','34413','34417') 
and a.data_date>='$101' and a.data_date<'$201' 
and a1.update_time=DATE_SUB(CURRENT_DATE(),3) 
and a2.update_time=DATE_SUB(CURRENT_DATE(),3) 
and a3.update_time=DATE_SUB(CURRENT_DATE(),3);


create table result_cur_curve_$1 as 
select tg_id, tg_no, data_id,  data_date , phase_flag ,1 as datetime,  i1 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,2 as datetime,  i2 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,3 as datetime,  i3 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,4 as datetime,  i4 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,5 as datetime,  i5 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,6 as datetime,  i6 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,7 as datetime,  i7 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,8 as datetime,  i8 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,9 as datetime,  i9 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,10 as datetime,  i10 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,11 as datetime,  i11 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,12 as datetime,  i12 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,13 as datetime,  i13 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,14 as datetime,  i14 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,15 as datetime,  i15 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,16 as datetime,  i16 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,17 as datetime,  i17 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,18 as datetime,  i18 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,19 as datetime,  i19 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,20 as datetime,  i20 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,21 as datetime,  i21 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,22 as datetime,  i22 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,23 as datetime,  i23 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,24 as datetime,  i24 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,25 as datetime,  i25 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,26 as datetime,  i26 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,27 as datetime,  i27 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,28 as datetime,  i28 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,29 as datetime,  i29 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,30 as datetime,  i30 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,31 as datetime,  i31 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,32 as datetime,  i32 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,33 as datetime,  i33 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,34 as datetime,  i34 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,35 as datetime,  i35 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,36 as datetime,  i36 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,37 as datetime,  i37 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,38 as datetime,  i38 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,39 as datetime,  i39 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,40 as datetime,  i40 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,41 as datetime,  i41 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,42 as datetime,  i42 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,43 as datetime,  i43 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,44 as datetime,  i44 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,45 as datetime,  i45 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,46 as datetime,  i46 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,47 as datetime,  i47 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,48 as datetime,  i48 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,49 as datetime,  i49 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,50 as datetime,  i50 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,51 as datetime,  i51 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,52 as datetime,  i52 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,53 as datetime,  i53 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,54 as datetime,  i54 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,55 as datetime,  i55 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,56 as datetime,  i56 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,57 as datetime,  i57 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,58 as datetime,  i58 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,59 as datetime,  i59 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,60 as datetime,  i60 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,61 as datetime,  i61 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,62 as datetime,  i62 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,63 as datetime,  i63 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,64 as datetime,  i64 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,65 as datetime,  i65 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,66 as datetime,  i66 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,67 as datetime,  i67 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,68 as datetime,  i68 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,69 as datetime,  i69 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,70 as datetime,  i70 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,71 as datetime,  i71 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,72 as datetime,  i72 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,73 as datetime,  i73 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,74 as datetime,  i74 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,75 as datetime,  i75 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,76 as datetime,  i76 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,77 as datetime,  i77 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,78 as datetime,  i78 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,79 as datetime,  i79 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,80 as datetime,  i80 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,81 as datetime,  i81 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,82 as datetime,  i82 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,83 as datetime,  i83 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,84 as datetime,  i84 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,85 as datetime,  i85 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,86 as datetime,  i86 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,87 as datetime,  i87 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,88 as datetime,  i88 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,89 as datetime,  i89 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,90 as datetime,  i90 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,91 as datetime,  i91 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,92 as datetime,  i92 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,93 as datetime,  i93 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,94 as datetime,  i94 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,95 as datetime,  i95 as data from data_cur_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,96 as datetime,  i96 as data from data_cur_$1;


create table data_power_$1 as 
select a1.tg_id, a1.tg_no, a.seq,a.data_id,a.area_code,a.data_date,a.phase_flag,a.ct,a.pt,a.org_no,a.col_addr,a.asset_no,a.meter_id,a.p1,a.p2,a.p3,a.p4,a.p5,a.p6,a.p7,a.p8,a.p9,a.p10,a.p11,a.p12,a.p13,a.p14,a.p15,a.p16,a.p17,a.p18,a.p19,a.p20,a.p21,a.p22,a.p23,a.p24,a.p25,a.p26,a.p27,a.p28,a.p29,a.p30,a.p31,a.p32,a.p33,a.p34,a.p35,a.p36,a.p37,a.p38,a.p39,a.p40,a.p41,a.p42,a.p43,a.p44,a.p45,a.p46,a.p47,a.p48,a.p49,a.p50,a.p51,a.p52,a.p53,a.p54,a.p55,a.p56,a.p57,a.p58,a.p59,a.p60,a.p61,a.p62,a.p63,a.p64,a.p65,a.p66,a.p67,a.p68,a.p69,a.p70,a.p71,a.p72,a.p73,a.p74,a.p75,a.p76,a.p77,a.p78,a.p79,a.p80,a.p81,a.p82,a.p83,a.p84,a.p85,a.p86,a.p87,a.p88,a.p89,a.p90,a.p91,a.p92,a.p93,a.p94,a.p95,a.p96,a.record_time,a.update_time
from root.dn_g_tg a1 left join root.dn_c_mp a2 on a1.tg_id = a2.tg_id and a2.type_code = '02'
left join root.dn_c_meter_mp_rela a3 on a2.mp_id = a3.mp_id
left join sjzl.r_data_mp a4 on a3.meter_id = a4.meter_id
left join sg_data_sharing_warehouse.e_mp_power_curve a on a4.id=a.data_id 
where substr(a1.org_no,0,5) in ('34401','34412','34413','34417') 
and a.data_date>='$101' and a.data_date<'$201' 
and a1.update_time=DATE_SUB(CURRENT_DATE(),3) 
and a2.update_time=DATE_SUB(CURRENT_DATE(),3) 
and a3.update_time=DATE_SUB(CURRENT_DATE(),3);


create table result_power_curve_$1 as 
select tg_id, tg_no, data_id,  data_date , phase_flag ,1 as datetime, p1 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,2 as datetime, p2 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,3 as datetime, p3 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,4 as datetime, p4 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,5 as datetime, p5 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,6 as datetime, p6 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,7 as datetime, p7 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,8 as datetime, p8 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,9 as datetime, p9 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,10 as datetime, p10 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,11 as datetime, p11 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,12 as datetime, p12 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,13 as datetime, p13 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,14 as datetime, p14 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,15 as datetime, p15 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,16 as datetime, p16 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,17 as datetime, p17 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,18 as datetime, p18 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,19 as datetime, p19 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,20 as datetime, p20 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,21 as datetime, p21 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,22 as datetime, p22 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,23 as datetime, p23 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,24 as datetime, p24 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,25 as datetime, p25 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,26 as datetime, p26 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,27 as datetime, p27 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,28 as datetime, p28 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,29 as datetime, p29 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,30 as datetime, p30 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,31 as datetime, p31 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,32 as datetime, p32 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,33 as datetime, p33 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,34 as datetime, p34 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,35 as datetime, p35 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,36 as datetime, p36 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,37 as datetime, p37 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,38 as datetime, p38 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,39 as datetime, p39 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,40 as datetime, p40 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,41 as datetime, p41 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,42 as datetime, p42 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,43 as datetime, p43 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,44 as datetime, p44 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,45 as datetime, p45 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,46 as datetime, p46 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,47 as datetime, p47 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,48 as datetime, p48 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,49 as datetime, p49 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,50 as datetime, p50 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,51 as datetime, p51 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,52 as datetime, p52 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,53 as datetime, p53 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,54 as datetime, p54 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,55 as datetime, p55 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,56 as datetime, p56 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,57 as datetime, p57 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,58 as datetime, p58 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,59 as datetime, p59 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,60 as datetime, p60 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,61 as datetime, p61 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,62 as datetime, p62 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,63 as datetime, p63 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,64 as datetime, p64 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,65 as datetime, p65 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,66 as datetime, p66 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,67 as datetime, p67 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,68 as datetime, p68 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,69 as datetime, p69 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,70 as datetime, p70 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,71 as datetime, p71 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,72 as datetime, p72 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,73 as datetime, p73 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,74 as datetime, p74 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,75 as datetime, p75 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,76 as datetime, p76 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,77 as datetime, p77 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,78 as datetime, p78 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,79 as datetime, p79 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,80 as datetime, p80 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,81 as datetime, p81 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,82 as datetime, p82 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,83 as datetime, p83 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,84 as datetime, p84 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,85 as datetime, p85 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,86 as datetime, p86 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,87 as datetime, p87 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,88 as datetime, p88 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,89 as datetime, p89 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,90 as datetime, p90 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,91 as datetime, p91 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,92 as datetime, p92 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,93 as datetime, p93 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,94 as datetime, p94 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,95 as datetime, p95 as data from data_power_$1
union all
select tg_id, tg_no, data_id,  data_date , phase_flag ,96 as datetime, p96 as data from data_power_$1;


create table result_factor_$1 as 
select tg_id, tg_no, data_id,  data_date , phase_flag , datetime,  cast(data as double)
from result_factor_curve_$1
where data != 'null';


create table result_cur_$1 as 
select tg_id, tg_no, data_id,  data_date , phase_flag , datetime,  cast(data as double)
from result_cur_curve_$1
where data != 'null';


create table result_power_$1 as 
select tg_id, tg_no, data_id,  data_date , phase_flag , datetime,  cast(data as double)
from result_power_curve_$1
where data != 'null';


drop table result_factor_curve_$1;
drop table result_cur_curve_$1;
drop table result_power_curve_$1;

"

echo "数据转换成功！"





