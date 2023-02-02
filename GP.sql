SELECT * FROM information_schema.tables LIMIT 10;

explain SELECT * FROM information_schema.tables limit 10;

drop table sandbox.campaign_00001;
truncate table sandbox.campaign_00001;
create table sandbox.campaign_00001 (campaign_cd varchar(100), inn varchar(12), product_id varchar(50), offer_desc varchar(500));
insert into sandbox.campaign_00001
values
('ЦА-КП01-0622',    '000000000000',    '1',   'Предложите продукт 1'),
('ЦА-КП01-0622',    '000000000001',    '2',   'Предложите продукт 2'),
('ЦА-КП01-0622',    '000000000002',    '3',   'Предложите продукт 3'),
('ЦА-КП01-0622',    '000000000000',    '4',   'Предложите продукт 4'),
('ЦА-КП01-0622',    '000000000001',    '5',   'Предложите продукт 5'),
('ЦА-КП01-0622',    '000000000002',    '6',   'Предложите продукт 6');
select * from sandbox.campaign_00001;

drop table sandbox.campaign_00002;
truncate table sandbox.campaign_00002;
create table sandbox.campaign_00002 (campaign_cd varchar(100), inn varchar(12), product_id varchar(50), offer_desc varchar(500), request_id integer);
insert into sandbox.campaign_00002
values
('ЦА-КП02-0622',    '000000000000',    '1',   'Предложите продукт 1', 1),
('ЦА-КП02-0622',    '000000000001',    '2',   'Предложите продукт 2', 1),
('ЦА-КП02-0622',    '000000000002',    '3',   'Предложите продукт 3', 1),
('ЦА-КП02-0622',    '000000000000',    '4',   'Предложите продукт 4', 1),
('ЦА-КП02-0622',    '000000000001',    '5',   'Предложите продукт 5', 1),
('ЦА-КП02-0622',    '000000000002',    '6',   'Предложите продукт 6', 1);
select * from sandbox.campaign_00002;

explain select * from sandbox.campaign_00001 t1 join sandbox.campaign_00002 t2 on t1.inn = t2.inn;
explain analyze select * from sandbox.campaign_00001 t1 join sandbox.campaign_00002 t2 on t1.inn = t2.inn;

drop table sandbox.stop_00001;
create table sandbox.stop_00001 (inn varchar(12));
insert into sandbox.stop_00001 values ('000000000000');
select * from sandbox.stop_00001;

drop table if exists prom.stop_dict;
truncate table prom.stop_dict;
create table prom.stop_dict(stop_id serial primary key
						   , stop_cd varchar(100)
						   , description varchar(500)
						   , stop_type varchar(100)
						   , schedule varchar(100)
						   , create_dt date not null default current_date
						   , refresh_dt date not null default current_date
						   , constraint stop_cd_unique unique (stop_cd));
insert into prom.stop_dict (stop_cd, description, stop_type, schedule)
values('Наличие счета', 'Тестовый стоп 01 по наличию счета','MATERIALIZED VIEW', '7d');
select * from prom.stop_dict;
create materialized view prom.stop_1 as
select inn from sandbox.stop_00001;

drop table prom.stop_repository;
truncate table prom.stop_repository;
create table prom.stop_repository (stop_id integer, inn varchar(12), constraint fk_stop_id foreign key(stop_id) references prom.stop_dict(stop_id));
insert into prom.stop_repository select 1, inn from sandbox.stop_00001;
select * from prom.stop_repository;

drop table prom.stop_list_dict;
create table prom.stop_list_dict (stop_list_cd varchar(100)
								, stop_cd varchar(100)
								, stop_arguments  varchar(200)
								, constraint stop_list_cd_unique unique (stop_list_cd, stop_cd)
								);

 insert into prom.stop_list_dict(stop_list_cd, stop_cd, stop_arguments)
VALUES
('Test stop list', 'Наличие счета', null),
('Test stop list', 'Наличие ЗП1', null),
('Test stop list', 'stop function test', '''{3}'',''{ЦА-КП03-0622}'',''{1,2}'''),
('Test stop list', 'stop view test 111', null),
('Test stop list', 'stop create test2', null),
('Test stop list', 'stop create mv test2', null);

select * from prom.stop_list_dict;

select * from pg_matviews where schemaname = 'prom' and matviewname = 'stop_5';
select * from prom.stop_dict where stop_id = 6;

create table prom.stop_14 as
select '000000000007' as inn;

drop view prom.pg_objects;
create or replace view prom.pg_objects as
select schemaname, matviewname as objectname, definition, 'MATERIALIZED VIEW' as object_type from pg_matviews where schemaname in ('sandbox', 'prom') union
select schemaname, viewname as objectname, definition, 'VIEW' as object_type from pg_views where schemaname in ('sandbox', 'prom') union
select schemaname, tablename as objectname, null as definition, 'TABLE' as object_type from pg_tables where schemaname in ('sandbox', 'prom') union
select distinct t2.nspname, t1.proname, first_value(t1.prosrc) over (partition by t1.proname order by t1.oid desc) as definition, 'FUNCTION' as object_type from pg_proc t1 join pg_catalog.pg_namespace t2 on t1.pronamespace = t2."oid" where t2.nspname in ('sandbox', 'prom');
select * from prom.pg_objects;

drop table sandbox.campaign_00003;
truncate table sandbox.campaign_00003;
create table sandbox.campaign_00003 (campaign_cd varchar(100), inn varchar(12), product_id varchar(50), offer_desc varchar(500), actip_type integer);
insert into sandbox.campaign_00003
values
('ЦА-КП03-0622',    '000000000000',    '1',   'Предложите продукт 1', 1),
('ЦА-КП03-0622',    '000000000001',    '2',   'Предложите продукт 2', 1),
('ЦА-КП03-0622',    '000000000002',    '3',   'Предложите продукт 3', 2),
('ЦА-КП04-0622',    '000000000000',    '4',   'Предложите продукт 4', 2),
('ЦА-КП04-0622',    '000000000001',    '5',   'Предложите продукт 5', 1),
('ЦА-КП04-0622',    '000000000002',    '6',   'Предложите продукт 6', 3),
('ЦА-КП04-0622',    '000000000003',    '4',   'Предложите продукт 4', 2),
('ЦА-КП04-0622',    '000000000004',    '5',   'Предложите продукт 5', 1),
('ЦА-КП04-0622',    '000000000005',    '6',   'Предложите продукт 6', 3),
('ЦА-КП04-0622',    '000000000025',    '6',   'Предложите продукт 6', 3);
select * from sandbox.campaign_00003;

create or replace function prom.stop_test_1(p1 text[], p2 text[], p3 int[])
 returns table (inn text) as
 $func$
 	select inn
 	  from sandbox.campaign_00003
 	 where 1 = 1
 	   and product_id = any($1)
 	   and campaign_cd = any($2)
 	   and actip_type = any($3)
 $func$ language sql;

select * from prom.stop_test_1('{1,2,3}','{ЦА-КП03-0622}','{1,2}');

select objectname from prom.pg_objects where object_type in ('TABLE', 'MATERIALIZED VIEW' ,'VIEW');

select * from pg_proc where proname = 'stop_test_1';

select definition from pg_views where schemaname = '{object_schema}' and viewname = '{object_name}'

select definition from prom.pg_objects where schemaname = 'prom' and objectname = 'stop_test_2';

select * from prom.stop_repository;
select * from sandbox.test_table_3;

drop table if exists prom.stop_target_dict;
truncate table prom.stop_target_dict;
create table prom.stop_target_dict(  target_id serial primary key
								   , target_cd varchar(100)
								   , create_dt date not null default current_date
								   , refresh_dt date not null default current_date
								   , constraint target_cd_unique unique (target_cd));
insert into prom.stop_target_dict(target_cd)
values('Таргет по кампании ЦА-КП05-0622');
select * from prom.stop_target_dict;

create or replace view sandbox.test_view_1 as
select * from sandbox.test_table_3;

drop table sandbox.test_table_3 restrict;

select * from prom.stop_dict
where stop_cd = any('{"Наличие счета", "Наличие ЗП1", "stop function test"}');

select * from prom.stop_1 union
select * from prom.stop_13 union
select * from prom.stop_16('{3}','{ЦА-КП03-0622}','{1,2}') union
select * from prom.stop_26;

select * from prom.stop1;
select * from prom.stop;

select * from prom.stop_repository where stop_id in (1, 13);

select distinct stop_id, inn from prom.stop_repository where stop_id in (1, 13) union
select distinct stop_id, inn from prom.stop_16('{3}','{ЦА-КП03-0622}','{1,2}') union
select distinct stop_id, inn from prom.stop_26;

select * from prom.stop_26;

with stops_query as (
select distinct stop_id, inn from prom.stop_repository where stop_id in (1, 13) union
select distinct 16 as stop_id, inn from prom.stop_16 ('{3}','{ЦА-КП03-0622}','{1,2}') union
select distinct 26 as stop_id, inn from prom.stop_26
),
pivot_stops_query as(
select inn
, max(case when stop_id = 1 then 1 else 0 end) as stop_1
, max(case when stop_id = 13 then 1 else 0 end) as stop_13
, max(case when stop_id = 16 then 1 else 0 end) as stop_16
, max(case when stop_id = 26 then 1 else 0 end) as stop_26
  from stops_query
group by inn
)
select t.*
, case when greatest(stop_1, stop_13, stop_16, stop_26) = 1 then 1 else 0 end as stop
, stop_1
, stop_13
, stop_16
, stop_26
  from sandbox.stop_target_7 as t
   left join pivot_stops_query as s
    on t.inn = s.inn;

select cur from prom.autocheck;
select last_value from prom.autocheck;

select nextval('autocheck');
select nextval('prom.autocheck'::regclass);
select lastval('prom.autocheck'::regclass);
select lastval('prom.stop_dict_stop_id_seq'::regclass);
select nextval('prom.stop_dict_stop_id_seq'::regclass);

select last_value from prom.stop_dict_stop_id_seq;

select case when True = False then 1 else 0 end;
select c1, case when c1=1 then 2 end as c2
from sandbox.stop_target_7
, lateral (select case when 1 = 1 then 1 end) as l(c1);

create table prom.ma_deal(inn varchar(12), host_prod_id varchar(20), deal_status_nm varchar(20));
create table prom.ma_product_offer(inn varchar(12), host_prod_id varchar(20), creation_dttm date);
create table prom.ma_task(inn varchar(12), host_prod_id varchar(20), create_dt date);
create table prom.ma_agreement(inn varchar(12), host_prod_id varchar(20), active_flg integer);
drop table prom.ma_unified_customer;
create table prom.ma_unified_customer(inn varchar(12), active_flg integer, crm_segment_type_nm varchar(20), kpp varchar(10), cusomer_id varchar(30), epk_id varchar(30));
create table prom.request_segment(request_id integer, crm_segment_type_nm varchar(20));

insert into prom.ma_unified_customer(inn, active_flg, crm_segment_type_nm, kpp, cusomer_id, epk_id)
values
('000000000000', 1, 'Микро', '00', '00', '00'),
('000000000001', 1, 'Микро', '01', '01', '01'),
('000000000002', 1, 'Микро', '02', '02', '02'),
('000000000003', 1, 'Малые', '03', '03', '03'),
('000000000004', 1, 'Малые', '04', '04', '04'),
('000000000005', 1, 'Малые', '05', '05', '05');

insert into prom.ma_unified_customer
VALUES
('0000000000', 1, 'Микро', '0', '0', '0'),
('0000000001', 1, 'Микро', '1', '1', '1'),
('0000000002', 1, 'Микро', '2', '2', '2'),
('0000000003', 1, 'Микро', '3', '3', '3'),
('0000000004', 1, 'Микро', '4', '4', '4'),
('0000000005', 1, 'Малые', '5', '5', '5'),
('0000000006', 1, 'Малые', '6', '6', '6'),
('0000000007', 1, 'Малые', '7', '7', '7'),
('0000000008', 1, 'Малые', '8', '8', '8'),
('0000000009', 1, 'Малые', '9', '9', '9');

insert into prom.request_segment(request_id, crm_segment_type_nm)
values
(1, 'Микро'),
(1, 'Малые');

drop table prom.insight_repository;
truncate table prom.insight_repository;

create table prom.insight_repository (
  INSIGHT_ID  serial primary key not null
, CREATION_DTTM Date  not null default current_date
, REQUEST_ID integer  not null
, SCENARIO_ID integer  not null
, INN Varchar (12)  not null
, KPP Varchar (10)  not null
, CUSTOMER_ID Varchar (30)  not null
, EPK_ID Varchar (30)  not null
, PRODUCT_ID Varchar (50)  not null
, INSIGHT_DESC Varchar (1000)  not null
, INSIGHT_SUM_VAL integer  not null
, INSIGHT_INCOME_VAL integer  not null
, INSIGHT_START_DT Date  not null
, INSIGHT_END_DT Date  not null
, constraint request_inn_product_unique unique (REQUEST_ID, INN, PRODUCT_ID)
);  

select * from prom.insight_repository where request_id = all('{"1"}');

drop table prom.insight_repository_extra;

create table prom.insight_repository_extra (
  INSIGHT_ID integer not null
, SCORE_VAL integer
, REASON integer
, OBJECT_ID  integer
, CONTAINER_ID integer
, NUM_ATTR_01 integer
, NUM_ATTR_02 integer
, NUM_ATTR_03 integer
, NUM_ATTR_04 integer
, NUM_ATTR_05 integer
, TEXT_ATTR_01 Varchar (100)
, TEXT_ATTR_02 Varchar (100)
, TEXT_ATTR_03 Varchar (100)
, TEXT_ATTR_04 Varchar (100)
, TEXT_ATTR_05 Varchar (1000)
, DATE_ATTR_01 Date
, DATE_ATTR_02 Date
, DATE_ATTR_03 Date
, DATE_ATTR_04 Date
, DATE_ATTR_05 Date
, constraint fk_insight_id foreign key(INSIGHT_ID) references prom.insight_repository(INSIGHT_ID)
);

create table prom.insight_repository_sbc (
  INSIGHT_ID integer not null
, CREATION_DTTM Date  not null default current_date
, REQUEST_ID integer  not null
, SCENARIO_ID integer  not null
, INN Varchar (12)  not null
, KPP Varchar (10)  not null
, CUSTOMER_ID Varchar (30)  not null
, EPK_ID Varchar (30)  not null
, PRODUCT_ID Varchar (50)  not null
, INSIGHT_DESC Varchar (1000)  not null
, INSIGHT_SUM_VAL integer  not null
, INSIGHT_INCOME_VAL integer  not null
, INSIGHT_START_DT Date  not null
, INSIGHT_END_DT Date  not null
, constraint request_inn_product_unique1 unique (REQUEST_ID, INN, PRODUCT_ID)
, constraint fk_insight_id foreign key(INSIGHT_ID) references prom.insight_repository(INSIGHT_ID)
);  

drop table prom.campaign_repository_sas;

create table prom.insight_repository_sas (
  SOURCE_CD_LV2 Varchar (100)
, INN Varchar (12)
, KPP Varchar (10)
, CRM_ID Varchar (20)
, EPK_ID Varchar (30)
, TASK_PRIORITY Varchar (30)
, TASK_TYPE Varchar (100)
, TASK_KM Varchar (20)
, PRODUCT_ID Varchar (30)
, OFFER_DESC_PP Varchar (1000)
, OFFER_DESC Varchar (1000)
, ENTITY_TYPE Varchar (100)
, OFFER_SUM_VAL integer
, OFFER_INCOME_VAL integer
, START_DT Date
, END_DT Date
, NUM_ATTR_01 integer
, NUM_ATTR_02 integer
, NUM_ATTR_03 integer
, TEXT_ATTR_01 Varchar (1000)
, TEXT_ATTR_02 Varchar (1000)
, TEXT_ATTR_03 Varchar (1000)
, DATE_ATTR_01 Date
, DATE_ATTR_02 Date
, DATE_ATTR_03 Date
);

select * from prom.campaign_repository;

drop table sandbox.campaign_1_test_rep_load;
truncate sandbox.campaign_1_test_rep_load;
create table sandbox.campaign_1_test_rep_load (
  REQUEST_ID integer
, SCENARIO_ID integer
, INN Varchar (12)
, KPP Varchar (10)
, CUSTOMER_ID Varchar (30)
, EPK_ID Varchar (30)
, PRODUCT_ID Varchar (50)
, INSIGHT_DESC Varchar (1000)
, INSIGHT_SUM_VAL integer
, INSIGHT_INCOME_VAL integer
, INSIGHT_START_DT Date
, INSIGHT_END_DT Date
); 

insert into sandbox.campaign_1_test_rep_load
VALUES
(1, 1, '0000000000', '0', '0', '0', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000001', '1', '1', '1', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000002', '2', '2', '2', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000003', '3', '3', '3', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000004', '4', '4', '4', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000005', '5', '5', '5', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000006', '6', '6', '6', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000007', '7', '7', '7', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000008', '8', '8', '8', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7),
(1, 1, '0000000009', '9', '9', '9', '0', 'Тестовые инсайты', 0, 0, current_date, current_date+7);
select * from sandbox.campaign_1_test_rep_load;