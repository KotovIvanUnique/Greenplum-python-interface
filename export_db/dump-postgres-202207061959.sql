--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4
-- Dumped by pg_dump version 14.4

-- Started on 2022-07-06 19:59:06

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'WIN1251';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 5 (class 2615 OID 18492)
-- Name: prom; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA prom;


ALTER SCHEMA prom OWNER TO postgres;

--
-- TOC entry 11 (class 2615 OID 18470)
-- Name: sandbox; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA sandbox;


ALTER SCHEMA sandbox OWNER TO postgres;

--
-- TOC entry 341 (class 1255 OID 24842)
-- Name: stop_16(text[], text[], integer[]); Type: FUNCTION; Schema: prom; Owner: postgres
--

CREATE FUNCTION prom.stop_16(p1 text[], p2 text[], p3 integer[]) RETURNS TABLE(inn text)
    LANGUAGE sql
    AS $_$
 	select inn
 	  from sandbox.campaign_00003
 	 where 1 = 1
 	   and product_id = any($1)
 	   and campaign_cd = any($2)
 	   and actip_type = any($3)
 $_$;


ALTER FUNCTION prom.stop_16(p1 text[], p2 text[], p3 integer[]) OWNER TO postgres;

--
-- TOC entry 342 (class 1255 OID 24823)
-- Name: stop_test_1(text, text); Type: FUNCTION; Schema: prom; Owner: postgres
--

CREATE FUNCTION prom.stop_test_1(p1 text, p2 text) RETURNS TABLE(inn text)
    LANGUAGE sql
    AS $_$
 	select inn
 	  from sandbox.campaign_00003
 	 where 1 = 1
 	   and product_id = any(string_to_array(replace($1, ', ', ','), ','))
 	   and campaign_cd = any(string_to_array(replace($2, ', ', ','), ','));
 $_$;


ALTER FUNCTION prom.stop_test_1(p1 text, p2 text) OWNER TO postgres;

--
-- TOC entry 343 (class 1255 OID 24832)
-- Name: stop_test_1(text[], text[], integer[]); Type: FUNCTION; Schema: prom; Owner: postgres
--

CREATE FUNCTION prom.stop_test_1(p1 text[], p2 text[], p3 integer[]) RETURNS TABLE(inn text)
    LANGUAGE sql
    AS $_$
 	select inn
 	  from sandbox.campaign_00003
 	 where 1 = 1
 	   and product_id = any($1)
 	   and campaign_cd = any($2)
 	   and actip_type = any($3)
 $_$;


ALTER FUNCTION prom.stop_test_1(p1 text[], p2 text[], p3 integer[]) OWNER TO postgres;

--
-- TOC entry 344 (class 1255 OID 24841)
-- Name: stop_test_2(text[], text[], integer[]); Type: FUNCTION; Schema: prom; Owner: postgres
--

CREATE FUNCTION prom.stop_test_2(p1 text[], p2 text[], p3 integer[]) RETURNS TABLE(inn text)
    LANGUAGE sql
    AS $_$
 	select inn
 	  from sandbox.campaign_00003
 	 where 1 = 1
 	   and product_id = any($1)
 	   and campaign_cd = any($2)
 	   and actip_type = any($3)
 $_$;


ALTER FUNCTION prom.stop_test_2(p1 text[], p2 text[], p3 integer[]) OWNER TO postgres;

--
-- TOC entry 288 (class 1259 OID 25199)
-- Name: autocheck; Type: SEQUENCE; Schema: prom; Owner: postgres
--

CREATE SEQUENCE prom.autocheck
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE prom.autocheck OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 216 (class 1259 OID 18508)
-- Name: campaign_repository; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.campaign_repository (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    num_attr_01 double precision,
    num_attr_02 double precision,
    num_attr_03 double precision,
    text_attr_01 character varying(500),
    text_attr_02 character varying(500),
    text_attr_03 character varying(500),
    date_attr_01 date,
    date_attr_02 date,
    date_attr_03 date
);


ALTER TABLE prom.campaign_repository OWNER TO postgres;

--
-- TOC entry 308 (class 1259 OID 25439)
-- Name: ma_agreement; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.ma_agreement (
    inn character varying(12),
    host_prod_id character varying(20),
    active_flg integer
);


ALTER TABLE prom.ma_agreement OWNER TO postgres;

--
-- TOC entry 305 (class 1259 OID 25430)
-- Name: ma_deal; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.ma_deal (
    inn character varying(12),
    host_prod_id character varying(20),
    deal_status_nm character varying(20)
);


ALTER TABLE prom.ma_deal OWNER TO postgres;

--
-- TOC entry 306 (class 1259 OID 25433)
-- Name: ma_product_offer; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.ma_product_offer (
    inn character varying(12),
    host_prod_id character varying(20),
    creation_dttm date
);


ALTER TABLE prom.ma_product_offer OWNER TO postgres;

--
-- TOC entry 307 (class 1259 OID 25436)
-- Name: ma_task; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.ma_task (
    inn character varying(12),
    host_prod_id character varying(20),
    create_dt date
);


ALTER TABLE prom.ma_task OWNER TO postgres;

--
-- TOC entry 309 (class 1259 OID 25442)
-- Name: ma_unified_customer; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.ma_unified_customer (
    inn character varying(12),
    active_flg integer,
    crm_segment_type_nm character varying(20)
);


ALTER TABLE prom.ma_unified_customer OWNER TO postgres;

--
-- TOC entry 247 (class 1259 OID 24835)
-- Name: pg_objects; Type: VIEW; Schema: prom; Owner: postgres
--

CREATE VIEW prom.pg_objects AS
 SELECT pg_matviews.schemaname,
    pg_matviews.matviewname AS objectname,
    pg_matviews.definition,
    'MATERIALIZED VIEW'::text AS object_type
   FROM pg_matviews
  WHERE (pg_matviews.schemaname = ANY (ARRAY['sandbox'::name, 'prom'::name]))
UNION
 SELECT pg_views.schemaname,
    pg_views.viewname AS objectname,
    pg_views.definition,
    'VIEW'::text AS object_type
   FROM pg_views
  WHERE (pg_views.schemaname = ANY (ARRAY['sandbox'::name, 'prom'::name]))
UNION
 SELECT pg_tables.schemaname,
    pg_tables.tablename AS objectname,
    NULL::text AS definition,
    'TABLE'::text AS object_type
   FROM pg_tables
  WHERE (pg_tables.schemaname = ANY (ARRAY['sandbox'::name, 'prom'::name]))
UNION
 SELECT DISTINCT t2.nspname AS schemaname,
    t1.proname AS objectname,
    first_value(t1.prosrc) OVER (PARTITION BY t1.proname ORDER BY t1.oid DESC) AS definition,
    'FUNCTION'::text AS object_type
   FROM (pg_proc t1
     JOIN pg_namespace t2 ON ((t1.pronamespace = t2.oid)))
  WHERE (t2.nspname = ANY (ARRAY['sandbox'::name, 'prom'::name]));


ALTER TABLE prom.pg_objects OWNER TO postgres;

--
-- TOC entry 310 (class 1259 OID 25445)
-- Name: request_segment; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.request_segment (
    request_id integer,
    crm_segment_type_nm character varying(20)
);


ALTER TABLE prom.request_segment OWNER TO postgres;

--
-- TOC entry 282 (class 1259 OID 25177)
-- Name: stop_1; Type: MATERIALIZED VIEW; Schema: prom; Owner: postgres
--

CREATE MATERIALIZED VIEW prom.stop_1 AS
 SELECT '000000000000'::text AS inn
UNION
 SELECT '000000000002'::text AS inn
  WITH NO DATA;


ALTER TABLE prom.stop_1 OWNER TO postgres;

--
-- TOC entry 236 (class 1259 OID 19106)
-- Name: stop_11; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_11 (
    inn text
);


ALTER TABLE prom.stop_11 OWNER TO postgres;

--
-- TOC entry 243 (class 1259 OID 24770)
-- Name: stop_13; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_13 (
    inn text
);


ALTER TABLE prom.stop_13 OWNER TO postgres;

--
-- TOC entry 244 (class 1259 OID 24775)
-- Name: stop_14; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_14 (
    inn text
);


ALTER TABLE prom.stop_14 OWNER TO postgres;

--
-- TOC entry 248 (class 1259 OID 24849)
-- Name: stop_19; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_19 (
    inn text
);


ALTER TABLE prom.stop_19 OWNER TO postgres;

--
-- TOC entry 240 (class 1259 OID 24711)
-- Name: test_table_3; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.test_table_3 (
    inn text
);


ALTER TABLE sandbox.test_table_3 OWNER TO postgres;

--
-- TOC entry 249 (class 1259 OID 24854)
-- Name: stop_20; Type: MATERIALIZED VIEW; Schema: prom; Owner: postgres
--

CREATE MATERIALIZED VIEW prom.stop_20 AS
 SELECT test_table_3.inn
   FROM sandbox.test_table_3
  WITH NO DATA;


ALTER TABLE prom.stop_20 OWNER TO postgres;

--
-- TOC entry 260 (class 1259 OID 25072)
-- Name: stop_26; Type: VIEW; Schema: prom; Owner: postgres
--

CREATE VIEW prom.stop_26 AS
 SELECT '000000000001'::text AS inn;


ALTER TABLE prom.stop_26 OWNER TO postgres;

--
-- TOC entry 264 (class 1259 OID 25099)
-- Name: stop_27; Type: VIEW; Schema: prom; Owner: postgres
--

CREATE VIEW prom.stop_27 AS
 SELECT '000000000001'::text AS inn;


ALTER TABLE prom.stop_27 OWNER TO postgres;

--
-- TOC entry 292 (class 1259 OID 25217)
-- Name: stop_30; Type: VIEW; Schema: prom; Owner: postgres
--

CREATE VIEW prom.stop_30 AS
 SELECT '000000000001'::text AS inn;


ALTER TABLE prom.stop_30 OWNER TO postgres;

--
-- TOC entry 293 (class 1259 OID 25221)
-- Name: stop_31; Type: VIEW; Schema: prom; Owner: postgres
--

CREATE VIEW prom.stop_31 AS
 SELECT '000000000001'::text AS inn;


ALTER TABLE prom.stop_31 OWNER TO postgres;

--
-- TOC entry 235 (class 1259 OID 19066)
-- Name: stop_4_src; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_4_src (
    inn text
);


ALTER TABLE prom.stop_4_src OWNER TO postgres;

--
-- TOC entry 234 (class 1259 OID 18953)
-- Name: stop_6_src; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_6_src (
    inn text
);


ALTER TABLE prom.stop_6_src OWNER TO postgres;

--
-- TOC entry 250 (class 1259 OID 24884)
-- Name: stop_9; Type: MATERIALIZED VIEW; Schema: prom; Owner: postgres
--

CREATE MATERIALIZED VIEW prom.stop_9 AS
 SELECT '000000000025'::text AS inn
  WITH NO DATA;


ALTER TABLE prom.stop_9 OWNER TO postgres;

--
-- TOC entry 245 (class 1259 OID 24785)
-- Name: stop_9_src; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_9_src (
    inn text
);


ALTER TABLE prom.stop_9_src OWNER TO postgres;

--
-- TOC entry 238 (class 1259 OID 19132)
-- Name: stop_dict; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_dict (
    stop_id integer NOT NULL,
    stop_cd character varying(100),
    description character varying(500),
    stop_type character varying(100),
    schedule character varying(100),
    create_dt date DEFAULT CURRENT_DATE NOT NULL,
    refresh_dt date DEFAULT CURRENT_DATE NOT NULL
);


ALTER TABLE prom.stop_dict OWNER TO postgres;

--
-- TOC entry 237 (class 1259 OID 19131)
-- Name: stop_dict_stop_id_seq; Type: SEQUENCE; Schema: prom; Owner: postgres
--

CREATE SEQUENCE prom.stop_dict_stop_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE prom.stop_dict_stop_id_seq OWNER TO postgres;

--
-- TOC entry 3830 (class 0 OID 0)
-- Dependencies: 237
-- Name: stop_dict_stop_id_seq; Type: SEQUENCE OWNED BY; Schema: prom; Owner: postgres
--

ALTER SEQUENCE prom.stop_dict_stop_id_seq OWNED BY prom.stop_dict.stop_id;


--
-- TOC entry 273 (class 1259 OID 25142)
-- Name: stop_none28; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_none28 (
    inn text
);


ALTER TABLE prom.stop_none28 OWNER TO postgres;

--
-- TOC entry 239 (class 1259 OID 19142)
-- Name: stop_repository; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_repository (
    stop_id integer,
    inn character varying(12)
);


ALTER TABLE prom.stop_repository OWNER TO postgres;

--
-- TOC entry 266 (class 1259 OID 25121)
-- Name: stop_target_11_flags; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_target_11_flags (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE prom.stop_target_11_flags OWNER TO postgres;

--
-- TOC entry 268 (class 1259 OID 25127)
-- Name: stop_target_12_flags; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_target_12_flags (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_27 integer
);


ALTER TABLE prom.stop_target_12_flags OWNER TO postgres;

--
-- TOC entry 270 (class 1259 OID 25133)
-- Name: stop_target_13_flags; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_target_13_flags (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE prom.stop_target_13_flags OWNER TO postgres;

--
-- TOC entry 261 (class 1259 OID 25087)
-- Name: stop_target_7_flags; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_target_7_flags (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    actip_type integer,
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_26 integer
);


ALTER TABLE prom.stop_target_7_flags OWNER TO postgres;

--
-- TOC entry 263 (class 1259 OID 25096)
-- Name: stop_target_9_flags; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_target_9_flags (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_26 integer
);


ALTER TABLE prom.stop_target_9_flags OWNER TO postgres;

--
-- TOC entry 253 (class 1259 OID 24912)
-- Name: stop_target_dict; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.stop_target_dict (
    target_id integer NOT NULL,
    target_cd character varying(100),
    create_dt date DEFAULT CURRENT_DATE NOT NULL,
    refresh_dt date DEFAULT CURRENT_DATE NOT NULL
);


ALTER TABLE prom.stop_target_dict OWNER TO postgres;

--
-- TOC entry 252 (class 1259 OID 24911)
-- Name: stop_target_dict_target_id_seq; Type: SEQUENCE; Schema: prom; Owner: postgres
--

CREATE SEQUENCE prom.stop_target_dict_target_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE prom.stop_target_dict_target_id_seq OWNER TO postgres;

--
-- TOC entry 3831 (class 0 OID 0)
-- Dependencies: 252
-- Name: stop_target_dict_target_id_seq; Type: SEQUENCE OWNED BY; Schema: prom; Owner: postgres
--

ALTER SEQUENCE prom.stop_target_dict_target_id_seq OWNED BY prom.stop_target_dict.target_id;


--
-- TOC entry 233 (class 1259 OID 18687)
-- Name: test; Type: TABLE; Schema: prom; Owner: postgres
--

CREATE TABLE prom.test (
    x integer
);


ALTER TABLE prom.test OWNER TO postgres;

--
-- TOC entry 289 (class 1259 OID 25200)
-- Name: autocheck; Type: SEQUENCE; Schema: sandbox; Owner: postgres
--

CREATE SEQUENCE sandbox.autocheck
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE sandbox.autocheck OWNER TO postgres;

--
-- TOC entry 296 (class 1259 OID 25231)
-- Name: autocheck_11; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_11 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.autocheck_11 OWNER TO postgres;

--
-- TOC entry 297 (class 1259 OID 25261)
-- Name: autocheck_21; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_21 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.autocheck_21 OWNER TO postgres;

--
-- TOC entry 298 (class 1259 OID 25348)
-- Name: autocheck_40; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_40 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.autocheck_40 OWNER TO postgres;

--
-- TOC entry 301 (class 1259 OID 25410)
-- Name: autocheck_54; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_54 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_54 OWNER TO postgres;

--
-- TOC entry 302 (class 1259 OID 25415)
-- Name: autocheck_55; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_55 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_55 OWNER TO postgres;

--
-- TOC entry 303 (class 1259 OID 25420)
-- Name: autocheck_56; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_56 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_56 OWNER TO postgres;

--
-- TOC entry 304 (class 1259 OID 25425)
-- Name: autocheck_57; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_57 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_57 OWNER TO postgres;

--
-- TOC entry 311 (class 1259 OID 25509)
-- Name: autocheck_70; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_70 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_70 OWNER TO postgres;

--
-- TOC entry 312 (class 1259 OID 25514)
-- Name: autocheck_71; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_71 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_71 OWNER TO postgres;

--
-- TOC entry 313 (class 1259 OID 25519)
-- Name: autocheck_72; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_72 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_72 OWNER TO postgres;

--
-- TOC entry 314 (class 1259 OID 25524)
-- Name: autocheck_73; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_73 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_73 OWNER TO postgres;

--
-- TOC entry 315 (class 1259 OID 25529)
-- Name: autocheck_74; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_74 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_74 OWNER TO postgres;

--
-- TOC entry 316 (class 1259 OID 25534)
-- Name: autocheck_75; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_75 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_75 OWNER TO postgres;

--
-- TOC entry 317 (class 1259 OID 25539)
-- Name: autocheck_76; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_76 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_76 OWNER TO postgres;

--
-- TOC entry 318 (class 1259 OID 25544)
-- Name: autocheck_77; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_77 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.autocheck_77 OWNER TO postgres;

--
-- TOC entry 320 (class 1259 OID 25559)
-- Name: autocheck_79; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_79 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    request_id integer
);


ALTER TABLE sandbox.autocheck_79 OWNER TO postgres;

--
-- TOC entry 321 (class 1259 OID 25570)
-- Name: autocheck_81; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_81 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    request_id integer
);


ALTER TABLE sandbox.autocheck_81 OWNER TO postgres;

--
-- TOC entry 322 (class 1259 OID 25575)
-- Name: autocheck_82; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_82 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    request_id integer
);


ALTER TABLE sandbox.autocheck_82 OWNER TO postgres;

--
-- TOC entry 323 (class 1259 OID 25580)
-- Name: autocheck_83; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.autocheck_83 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    request_id integer
);


ALTER TABLE sandbox.autocheck_83 OWNER TO postgres;

--
-- TOC entry 215 (class 1259 OID 18498)
-- Name: campaign_00001; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.campaign_00001 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500)
);


ALTER TABLE sandbox.campaign_00001 OWNER TO postgres;

--
-- TOC entry 319 (class 1259 OID 25554)
-- Name: campaign_00002; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.campaign_00002 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    request_id integer
);


ALTER TABLE sandbox.campaign_00002 OWNER TO postgres;

--
-- TOC entry 246 (class 1259 OID 24824)
-- Name: campaign_00003; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.campaign_00003 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    actip_type integer
);


ALTER TABLE sandbox.campaign_00003 OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 18519)
-- Name: stop_00001; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_00001 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_00001 OWNER TO postgres;

--
-- TOC entry 265 (class 1259 OID 25103)
-- Name: stop_target_11; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_11 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_11 OWNER TO postgres;

--
-- TOC entry 267 (class 1259 OID 25124)
-- Name: stop_target_12; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_12 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_12 OWNER TO postgres;

--
-- TOC entry 269 (class 1259 OID 25130)
-- Name: stop_target_13; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_13 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_13 OWNER TO postgres;

--
-- TOC entry 271 (class 1259 OID 25136)
-- Name: stop_target_14; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_14 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_14 OWNER TO postgres;

--
-- TOC entry 272 (class 1259 OID 25139)
-- Name: stop_target_14_flags; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_14_flags (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_14_flags OWNER TO postgres;

--
-- TOC entry 274 (class 1259 OID 25147)
-- Name: stop_target_15; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_15 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_15 OWNER TO postgres;

--
-- TOC entry 276 (class 1259 OID 25159)
-- Name: stop_target_16; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_16 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_16 OWNER TO postgres;

--
-- TOC entry 278 (class 1259 OID 25165)
-- Name: stop_target_17; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_17 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_17 OWNER TO postgres;

--
-- TOC entry 280 (class 1259 OID 25171)
-- Name: stop_target_18; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_18 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_18 OWNER TO postgres;

--
-- TOC entry 283 (class 1259 OID 25183)
-- Name: stop_target_19; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_19 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_19 OWNER TO postgres;

--
-- TOC entry 285 (class 1259 OID 25189)
-- Name: stop_target_20; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_20 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_20 OWNER TO postgres;

--
-- TOC entry 290 (class 1259 OID 25201)
-- Name: stop_target_21; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_21 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_21 OWNER TO postgres;

--
-- TOC entry 294 (class 1259 OID 25225)
-- Name: stop_target_22; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_22 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_22 OWNER TO postgres;

--
-- TOC entry 299 (class 1259 OID 25364)
-- Name: stop_target_23; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_23 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_23 OWNER TO postgres;

--
-- TOC entry 254 (class 1259 OID 24938)
-- Name: stop_target_7; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_7 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    actip_type integer
);


ALTER TABLE sandbox.stop_target_7 OWNER TO postgres;

--
-- TOC entry 255 (class 1259 OID 24943)
-- Name: stop_target_8; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_8 (
    campaign_cd character varying(100),
    inn character varying(12),
    product_id character varying(50),
    offer_desc character varying(500),
    actip_type integer
);


ALTER TABLE sandbox.stop_target_8 OWNER TO postgres;

--
-- TOC entry 262 (class 1259 OID 25093)
-- Name: stop_target_9; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_9 (
    inn character varying(12)
);


ALTER TABLE sandbox.stop_target_9 OWNER TO postgres;

--
-- TOC entry 275 (class 1259 OID 25150)
-- Name: stop_target_flags_15; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_15 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_15 OWNER TO postgres;

--
-- TOC entry 277 (class 1259 OID 25162)
-- Name: stop_target_flags_16; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_16 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_16 OWNER TO postgres;

--
-- TOC entry 279 (class 1259 OID 25168)
-- Name: stop_target_flags_17; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_17 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_17 OWNER TO postgres;

--
-- TOC entry 281 (class 1259 OID 25174)
-- Name: stop_target_flags_18; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_18 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_18 OWNER TO postgres;

--
-- TOC entry 284 (class 1259 OID 25186)
-- Name: stop_target_flags_19; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_19 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_19 OWNER TO postgres;

--
-- TOC entry 286 (class 1259 OID 25192)
-- Name: stop_target_flags_20; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_20 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_20 OWNER TO postgres;

--
-- TOC entry 291 (class 1259 OID 25204)
-- Name: stop_target_flags_21; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_21 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_21 OWNER TO postgres;

--
-- TOC entry 295 (class 1259 OID 25228)
-- Name: stop_target_flags_22; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_22 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_22 OWNER TO postgres;

--
-- TOC entry 300 (class 1259 OID 25367)
-- Name: stop_target_flags_23; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.stop_target_flags_23 (
    inn character varying(12),
    stop integer,
    stop_1 integer,
    stop_13 integer,
    stop_16 integer,
    stop_19 integer,
    stop_20 integer,
    stop_27 integer
);


ALTER TABLE sandbox.stop_target_flags_23 OWNER TO postgres;

--
-- TOC entry 287 (class 1259 OID 25195)
-- Name: test_format; Type: VIEW; Schema: sandbox; Owner: postgres
--

CREATE VIEW sandbox.test_format AS
 SELECT 1 AS c1,
    2 AS c2,
    3 AS c3,
    4 AS c4;


ALTER TABLE sandbox.test_format OWNER TO postgres;

--
-- TOC entry 251 (class 1259 OID 24891)
-- Name: test_mv_1; Type: MATERIALIZED VIEW; Schema: sandbox; Owner: postgres
--

CREATE MATERIALIZED VIEW sandbox.test_mv_1 AS
 SELECT '0000000003'::text AS inn
  WITH NO DATA;


ALTER TABLE sandbox.test_mv_1 OWNER TO postgres;

--
-- TOC entry 256 (class 1259 OID 24948)
-- Name: test_mv_2; Type: MATERIALIZED VIEW; Schema: sandbox; Owner: postgres
--

CREATE MATERIALIZED VIEW sandbox.test_mv_2 AS
 SELECT '0000000003'::text AS inn
  WITH NO DATA;


ALTER TABLE sandbox.test_mv_2 OWNER TO postgres;

--
-- TOC entry 241 (class 1259 OID 24716)
-- Name: test_table_1; Type: MATERIALIZED VIEW; Schema: sandbox; Owner: postgres
--

CREATE MATERIALIZED VIEW sandbox.test_table_1 AS
 SELECT '2222222222'::text AS inn
  WITH NO DATA;


ALTER TABLE sandbox.test_table_1 OWNER TO postgres;

--
-- TOC entry 242 (class 1259 OID 24722)
-- Name: test_table_2; Type: MATERIALIZED VIEW; Schema: sandbox; Owner: postgres
--

CREATE MATERIALIZED VIEW sandbox.test_table_2 AS
 SELECT t.campaign_cd,
    t.inn,
    t.product_id,
    t.offer_desc
   FROM ( VALUES ('ЦА-КП02-0622'::text,'000000000000'::text,'1'::text,'Предложите продукт 1'::text), ('ЦА-КП02-0622'::text,'000000000001'::text,'2'::text,'Предложите продукт 2'::text), ('ЦА-КП02-0622'::text,'000000000002'::text,'3'::text,'Предложите продукт 3'::text), ('ЦА-КП02-0622'::text,'000000000000'::text,'4'::text,'Предложите продукт 4'::text), ('ЦА-КП02-0622'::text,'000000000001'::text,'5'::text,'Предложите продукт 5'::text), ('ЦА-КП02-0622'::text,'000000000002'::text,'6'::text,'Предложите продукт 6'::text)) t(campaign_cd, inn, product_id, offer_desc)
  WITH NO DATA;


ALTER TABLE sandbox.test_table_2 OWNER TO postgres;

--
-- TOC entry 257 (class 1259 OID 24954)
-- Name: test_table_4; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.test_table_4 (
    inn text
);


ALTER TABLE sandbox.test_table_4 OWNER TO postgres;

--
-- TOC entry 258 (class 1259 OID 25043)
-- Name: test_table_5; Type: TABLE; Schema: sandbox; Owner: postgres
--

CREATE TABLE sandbox.test_table_5 (
    inn text
);


ALTER TABLE sandbox.test_table_5 OWNER TO postgres;

--
-- TOC entry 259 (class 1259 OID 25060)
-- Name: test_view_1; Type: VIEW; Schema: sandbox; Owner: postgres
--

CREATE VIEW sandbox.test_view_1 AS
 SELECT test_table_3.inn
   FROM sandbox.test_table_3;


ALTER TABLE sandbox.test_view_1 OWNER TO postgres;

--
-- TOC entry 3570 (class 2604 OID 19135)
-- Name: stop_dict stop_id; Type: DEFAULT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_dict ALTER COLUMN stop_id SET DEFAULT nextval('prom.stop_dict_stop_id_seq'::regclass);


--
-- TOC entry 3573 (class 2604 OID 24915)
-- Name: stop_target_dict target_id; Type: DEFAULT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_target_dict ALTER COLUMN target_id SET DEFAULT nextval('prom.stop_target_dict_target_id_seq'::regclass);


--
-- TOC entry 3739 (class 0 OID 18508)
-- Dependencies: 216
-- Data for Name: campaign_repository; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO prom.campaign_repository VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


--
-- TOC entry 3809 (class 0 OID 25439)
-- Dependencies: 308
-- Data for Name: ma_agreement; Type: TABLE DATA; Schema: prom; Owner: postgres
--



--
-- TOC entry 3806 (class 0 OID 25430)
-- Dependencies: 305
-- Data for Name: ma_deal; Type: TABLE DATA; Schema: prom; Owner: postgres
--



--
-- TOC entry 3807 (class 0 OID 25433)
-- Dependencies: 306
-- Data for Name: ma_product_offer; Type: TABLE DATA; Schema: prom; Owner: postgres
--



--
-- TOC entry 3808 (class 0 OID 25436)
-- Dependencies: 307
-- Data for Name: ma_task; Type: TABLE DATA; Schema: prom; Owner: postgres
--



--
-- TOC entry 3810 (class 0 OID 25442)
-- Dependencies: 309
-- Data for Name: ma_unified_customer; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.ma_unified_customer VALUES ('000000000000', 1, 'Микро');
INSERT INTO prom.ma_unified_customer VALUES ('000000000001', 1, 'Микро');
INSERT INTO prom.ma_unified_customer VALUES ('000000000002', 1, 'Микро');
INSERT INTO prom.ma_unified_customer VALUES ('000000000000', 1, 'Малые');
INSERT INTO prom.ma_unified_customer VALUES ('000000000001', 1, 'Малые');
INSERT INTO prom.ma_unified_customer VALUES ('000000000002', 1, 'Малые');


--
-- TOC entry 3811 (class 0 OID 25445)
-- Dependencies: 310
-- Data for Name: request_segment; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.request_segment VALUES (1, 'Микро');
INSERT INTO prom.request_segment VALUES (1, 'Малые');


--
-- TOC entry 3744 (class 0 OID 19106)
-- Dependencies: 236
-- Data for Name: stop_11; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_11 VALUES ('000000000007');


--
-- TOC entry 3751 (class 0 OID 24770)
-- Dependencies: 243
-- Data for Name: stop_13; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_13 VALUES ('000000000005');
INSERT INTO prom.stop_13 VALUES ('000000000025');


--
-- TOC entry 3752 (class 0 OID 24775)
-- Dependencies: 244
-- Data for Name: stop_14; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_14 VALUES ('000000000005');


--
-- TOC entry 3755 (class 0 OID 24849)
-- Dependencies: 248
-- Data for Name: stop_19; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_19 VALUES ('0000000000');
INSERT INTO prom.stop_19 VALUES ('000000000000');
INSERT INTO prom.stop_19 VALUES ('000000000001');
INSERT INTO prom.stop_19 VALUES ('000000000002');
INSERT INTO prom.stop_19 VALUES ('000000000000');
INSERT INTO prom.stop_19 VALUES ('000000000001');
INSERT INTO prom.stop_19 VALUES ('000000000002');
INSERT INTO prom.stop_19 VALUES ('000000000003');
INSERT INTO prom.stop_19 VALUES ('000000000004');
INSERT INTO prom.stop_19 VALUES ('000000000005');


--
-- TOC entry 3743 (class 0 OID 19066)
-- Dependencies: 235
-- Data for Name: stop_4_src; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_4_src VALUES ('000000000025');


--
-- TOC entry 3742 (class 0 OID 18953)
-- Dependencies: 234
-- Data for Name: stop_6_src; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_6_src VALUES ('000000000025');


--
-- TOC entry 3753 (class 0 OID 24785)
-- Dependencies: 245
-- Data for Name: stop_9_src; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_9_src VALUES ('000000000005');


--
-- TOC entry 3746 (class 0 OID 19132)
-- Dependencies: 238
-- Data for Name: stop_dict; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_dict VALUES (1, 'Наличие счета', 'Стоп по клиентам, имующим в наличии активный расчетный счет', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (13, 'Наличие ЗП1', 'Тестовый стоп 05', 'TABLE', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (14, 'Наличие ЗП2', 'Тестовый стоп 05', 'TABLE', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (16, 'stop function test', 'Тестовый стоп для проверки работы стопов-функций', 'FUNCTION', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (17, 'stop create test', 'Тестовый стоп для проверки работы создания стопов', 'TABLE', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (18, 'stop create test1', 'Тестовый стоп для проверки работы создания стопов', 'TABLE', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (19, 'stop create test2', 'Тестовый стоп для проверки работы создания стопов', 'TABLE', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (20, 'stop create mv test2', 'Тестовый стоп для проверки работы создания стопов', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (21, 'stop create mv test3', 'Тестовый стоп для проверки работы создания стопов', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (22, 'stop create mv test4', 'Тестовый стоп для проверки работы создания стопов', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (23, 'stop create mv test5', 'Тестовый стоп для проверки работы создания стопов', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (24, '1', 'Тестовый стоп для проверки работы создания стопов', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_dict VALUES (26, 'stop create test 111', 'Тестовый стоп для проверки работы создания стопов', 'VIEW', '7d', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_dict VALUES (27, 'stop view test 111', 'Тестовый стоп для проверки работы создания view', 'VIEW', '7d', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_dict VALUES (30, 'stop view test 112', 'Тестовый стоп для проверки работы создания №1', 'VIEW', '7d', '2022-07-04', '2022-07-04');
INSERT INTO prom.stop_dict VALUES (31, 'stop view test 113', 'Тестовый стоп для проверки работы создания №1', 'VIEW', '7d', '2022-07-04', '2022-07-04');
INSERT INTO prom.stop_dict VALUES (9, 'Наличие ЗП', 'Тестовый стоп 05', 'MATERIALIZED VIEW', '7d', '2022-06-28', '2022-07-05');


--
-- TOC entry 3777 (class 0 OID 25142)
-- Dependencies: 273
-- Data for Name: stop_none28; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_none28 VALUES ('000000000001');


--
-- TOC entry 3747 (class 0 OID 19142)
-- Dependencies: 239
-- Data for Name: stop_repository; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_repository VALUES (1, '000000000000');
INSERT INTO prom.stop_repository VALUES (1, '000000000000');
INSERT INTO prom.stop_repository VALUES (9, '000000000025');
INSERT INTO prom.stop_repository VALUES (14, '000000000005');
INSERT INTO prom.stop_repository VALUES (13, '000000000005');
INSERT INTO prom.stop_repository VALUES (19, '0000000000');
INSERT INTO prom.stop_repository VALUES (19, '000000000000');
INSERT INTO prom.stop_repository VALUES (19, '000000000001');
INSERT INTO prom.stop_repository VALUES (19, '000000000002');
INSERT INTO prom.stop_repository VALUES (19, '000000000000');
INSERT INTO prom.stop_repository VALUES (19, '000000000001');
INSERT INTO prom.stop_repository VALUES (19, '000000000002');
INSERT INTO prom.stop_repository VALUES (19, '0000000000');
INSERT INTO prom.stop_repository VALUES (19, '000000000000');
INSERT INTO prom.stop_repository VALUES (19, '000000000001');
INSERT INTO prom.stop_repository VALUES (19, '000000000002');
INSERT INTO prom.stop_repository VALUES (19, '000000000000');
INSERT INTO prom.stop_repository VALUES (19, '000000000001');
INSERT INTO prom.stop_repository VALUES (19, '000000000002');
INSERT INTO prom.stop_repository VALUES (19, '000000000003');
INSERT INTO prom.stop_repository VALUES (19, '000000000004');
INSERT INTO prom.stop_repository VALUES (19, '000000000005');
INSERT INTO prom.stop_repository VALUES (13, '000000000005');
INSERT INTO prom.stop_repository VALUES (13, '000000000025');
INSERT INTO prom.stop_repository VALUES (1, '000000000000');
INSERT INTO prom.stop_repository VALUES (1, '000000000002');
INSERT INTO prom.stop_repository VALUES (20, '0000000000');
INSERT INTO prom.stop_repository VALUES (20, '000000000000');
INSERT INTO prom.stop_repository VALUES (20, '000000000001');
INSERT INTO prom.stop_repository VALUES (20, '000000000002');
INSERT INTO prom.stop_repository VALUES (20, '000000000000');
INSERT INTO prom.stop_repository VALUES (20, '000000000001');
INSERT INTO prom.stop_repository VALUES (20, '000000000002');


--
-- TOC entry 3770 (class 0 OID 25121)
-- Dependencies: 266
-- Data for Name: stop_target_11_flags; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_target_11_flags VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO prom.stop_target_11_flags VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO prom.stop_target_11_flags VALUES ('000000000000', 1, 1, 0, 0, 1, 0);


--
-- TOC entry 3772 (class 0 OID 25127)
-- Dependencies: 268
-- Data for Name: stop_target_12_flags; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_target_12_flags VALUES ('000000000002', 1, 0, 0, 1, 0);
INSERT INTO prom.stop_target_12_flags VALUES ('000000000001', 1, 0, 0, 0, 1);
INSERT INTO prom.stop_target_12_flags VALUES ('000000000000', 1, 1, 0, 0, 0);


--
-- TOC entry 3774 (class 0 OID 25133)
-- Dependencies: 270
-- Data for Name: stop_target_13_flags; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_target_13_flags VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO prom.stop_target_13_flags VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO prom.stop_target_13_flags VALUES ('000000000000', 1, 1, 0, 0, 1, 0);


--
-- TOC entry 3766 (class 0 OID 25087)
-- Dependencies: 261
-- Data for Name: stop_target_7_flags; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_target_7_flags VALUES ('ЦА-КП03-0622', '000000000000', '1', 'Предложите продукт 1', 1, 1, 1, 0, 0, 0);
INSERT INTO prom.stop_target_7_flags VALUES ('ЦА-КП03-0622', '000000000001', '2', 'Предложите продукт 2', 1, 1, 0, 0, 0, 1);
INSERT INTO prom.stop_target_7_flags VALUES ('ЦА-КП03-0622', '000000000002', '3', 'Предложите продукт 3', 2, 1, 0, 0, 1, 0);
INSERT INTO prom.stop_target_7_flags VALUES ('ЦА-КП04-0622', '000000000000', '4', 'Предложите продукт 4', 2, 1, 1, 0, 0, 0);
INSERT INTO prom.stop_target_7_flags VALUES ('ЦА-КП04-0622', '000000000001', '5', 'Предложите продукт 5', 1, 1, 0, 0, 0, 1);
INSERT INTO prom.stop_target_7_flags VALUES ('ЦА-КП04-0622', '000000000002', '6', 'Предложите продукт 6', 3, 1, 0, 0, 1, 0);


--
-- TOC entry 3768 (class 0 OID 25096)
-- Dependencies: 263
-- Data for Name: stop_target_9_flags; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_target_9_flags VALUES ('000000000002', 1, 0, 0, 1, 0);
INSERT INTO prom.stop_target_9_flags VALUES ('000000000001', 1, 0, 0, 0, 1);
INSERT INTO prom.stop_target_9_flags VALUES ('000000000000', 1, 1, 0, 0, 0);


--
-- TOC entry 3760 (class 0 OID 24912)
-- Dependencies: 253
-- Data for Name: stop_target_dict; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.stop_target_dict VALUES (1, 'Таргет по кампании ЦА-КП05-0622', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_target_dict VALUES (2, 'Тестовый таргет campaign_00001', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_target_dict VALUES (7, 'Тестовый таргет campaign_00003 №2', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_target_dict VALUES (8, 'Тестовый таргет campaign_00003 №3', '2022-06-28', '2022-06-28');
INSERT INTO prom.stop_target_dict VALUES (9, 'Тестовый таргет campaign_00003 №5', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_target_dict VALUES (11, 'Тестовый таргет campaign_00003 №6', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_target_dict VALUES (12, 'Тестовый таргет campaign_00003 №7', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_target_dict VALUES (13, 'Тестовый таргет campaign_00003 №8', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_target_dict VALUES (14, 'Тестовый таргет campaign_00003 №9', '2022-06-29', '2022-06-29');
INSERT INTO prom.stop_target_dict VALUES (15, 'Тестовый таргет campaign_00003 №10', '2022-06-30', '2022-06-30');
INSERT INTO prom.stop_target_dict VALUES (16, 'Тестовый таргет campaign_00003 №11', '2022-07-01', '2022-07-01');
INSERT INTO prom.stop_target_dict VALUES (17, 'Тестовый таргет campaign_00003 №12', '2022-07-01', '2022-07-01');
INSERT INTO prom.stop_target_dict VALUES (18, 'Тестовый таргет campaign_00003 №13', '2022-07-01', '2022-07-01');
INSERT INTO prom.stop_target_dict VALUES (19, 'Тестовый таргет campaign_00003 №14', '2022-07-01', '2022-07-01');
INSERT INTO prom.stop_target_dict VALUES (20, 'Тестовый таргет campaign_00003 №15', '2022-07-01', '2022-07-01');
INSERT INTO prom.stop_target_dict VALUES (21, 'Тестовый таргет campaign_00003 №16', '2022-07-04', '2022-07-04');
INSERT INTO prom.stop_target_dict VALUES (22, 'Тестовый таргет campaign_00003 №17', '2022-07-04', '2022-07-04');
INSERT INTO prom.stop_target_dict VALUES (23, 'Тестовый таргет campaign_00003 №18', '2022-07-05', '2022-07-05');


--
-- TOC entry 3741 (class 0 OID 18687)
-- Dependencies: 233
-- Data for Name: test; Type: TABLE DATA; Schema: prom; Owner: postgres
--

INSERT INTO prom.test VALUES (1);
INSERT INTO prom.test VALUES (1);
INSERT INTO prom.test VALUES (1);


--
-- TOC entry 3797 (class 0 OID 25231)
-- Dependencies: 296
-- Data for Name: autocheck_11; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_11 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.autocheck_11 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.autocheck_11 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.autocheck_11 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.autocheck_11 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.autocheck_11 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.autocheck_11 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3798 (class 0 OID 25261)
-- Dependencies: 297
-- Data for Name: autocheck_21; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_21 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.autocheck_21 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.autocheck_21 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.autocheck_21 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.autocheck_21 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.autocheck_21 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.autocheck_21 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3799 (class 0 OID 25348)
-- Dependencies: 298
-- Data for Name: autocheck_40; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_40 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.autocheck_40 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.autocheck_40 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.autocheck_40 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.autocheck_40 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.autocheck_40 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.autocheck_40 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3802 (class 0 OID 25410)
-- Dependencies: 301
-- Data for Name: autocheck_54; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_54 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_54 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_54 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_54 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_54 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_54 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3803 (class 0 OID 25415)
-- Dependencies: 302
-- Data for Name: autocheck_55; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_55 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_55 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_55 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_55 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_55 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_55 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3804 (class 0 OID 25420)
-- Dependencies: 303
-- Data for Name: autocheck_56; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_56 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_56 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_56 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_56 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_56 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_56 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3805 (class 0 OID 25425)
-- Dependencies: 304
-- Data for Name: autocheck_57; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_57 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_57 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_57 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_57 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_57 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_57 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3812 (class 0 OID 25509)
-- Dependencies: 311
-- Data for Name: autocheck_70; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_70 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_70 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_70 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_70 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_70 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_70 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3813 (class 0 OID 25514)
-- Dependencies: 312
-- Data for Name: autocheck_71; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_71 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_71 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_71 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_71 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_71 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_71 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3814 (class 0 OID 25519)
-- Dependencies: 313
-- Data for Name: autocheck_72; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_72 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_72 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_72 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_72 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_72 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_72 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3815 (class 0 OID 25524)
-- Dependencies: 314
-- Data for Name: autocheck_73; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_73 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_73 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_73 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_73 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_73 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_73 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3816 (class 0 OID 25529)
-- Dependencies: 315
-- Data for Name: autocheck_74; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_74 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_74 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_74 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_74 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_74 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_74 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3817 (class 0 OID 25534)
-- Dependencies: 316
-- Data for Name: autocheck_75; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_75 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_75 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_75 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_75 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_75 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_75 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3818 (class 0 OID 25539)
-- Dependencies: 317
-- Data for Name: autocheck_76; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_76 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_76 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_76 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_76 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_76 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_76 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3819 (class 0 OID 25544)
-- Dependencies: 318
-- Data for Name: autocheck_77; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_77 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.autocheck_77 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.autocheck_77 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.autocheck_77 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.autocheck_77 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.autocheck_77 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3821 (class 0 OID 25559)
-- Dependencies: 320
-- Data for Name: autocheck_79; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_79 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.autocheck_79 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.autocheck_79 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', 1);
INSERT INTO sandbox.autocheck_79 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', 1);
INSERT INTO sandbox.autocheck_79 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.autocheck_79 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', 1);


--
-- TOC entry 3822 (class 0 OID 25570)
-- Dependencies: 321
-- Data for Name: autocheck_81; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_81 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.autocheck_81 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.autocheck_81 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', 1);
INSERT INTO sandbox.autocheck_81 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', 1);
INSERT INTO sandbox.autocheck_81 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.autocheck_81 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', 1);


--
-- TOC entry 3823 (class 0 OID 25575)
-- Dependencies: 322
-- Data for Name: autocheck_82; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_82 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.autocheck_82 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.autocheck_82 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', 1);
INSERT INTO sandbox.autocheck_82 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', 1);
INSERT INTO sandbox.autocheck_82 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.autocheck_82 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', 1);


--
-- TOC entry 3824 (class 0 OID 25580)
-- Dependencies: 323
-- Data for Name: autocheck_83; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.autocheck_83 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.autocheck_83 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.autocheck_83 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', 1);
INSERT INTO sandbox.autocheck_83 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', 1);
INSERT INTO sandbox.autocheck_83 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.autocheck_83 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', 1);


--
-- TOC entry 3738 (class 0 OID 18498)
-- Dependencies: 215
-- Data for Name: campaign_00001; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.campaign_00001 VALUES ('ЦА-КП01-0622', '000000000000', '1', 'Предложите продукт 1');
INSERT INTO sandbox.campaign_00001 VALUES ('ЦА-КП01-0622', '000000000001', '2', 'Предложите продукт 2');
INSERT INTO sandbox.campaign_00001 VALUES ('ЦА-КП01-0622', '000000000002', '3', 'Предложите продукт 3');
INSERT INTO sandbox.campaign_00001 VALUES ('ЦА-КП01-0622', '000000000000', '4', 'Предложите продукт 4');
INSERT INTO sandbox.campaign_00001 VALUES ('ЦА-КП01-0622', '000000000001', '5', 'Предложите продукт 5');
INSERT INTO sandbox.campaign_00001 VALUES ('ЦА-КП01-0622', '000000000002', '6', 'Предложите продукт 6');


--
-- TOC entry 3820 (class 0 OID 25554)
-- Dependencies: 319
-- Data for Name: campaign_00002; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.campaign_00002 VALUES ('ЦА-КП02-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.campaign_00002 VALUES ('ЦА-КП02-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.campaign_00002 VALUES ('ЦА-КП02-0622', '000000000002', '3', 'Предложите продукт 3', 1);
INSERT INTO sandbox.campaign_00002 VALUES ('ЦА-КП02-0622', '000000000000', '4', 'Предложите продукт 4', 1);
INSERT INTO sandbox.campaign_00002 VALUES ('ЦА-КП02-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.campaign_00002 VALUES ('ЦА-КП02-0622', '000000000002', '6', 'Предложите продукт 6', 1);


--
-- TOC entry 3754 (class 0 OID 24824)
-- Dependencies: 246
-- Data for Name: campaign_00003; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП03-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП03-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП03-0622', '000000000002', '3', 'Предложите продукт 3', 2);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000000', '4', 'Предложите продукт 4', 2);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000002', '6', 'Предложите продукт 6', 3);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000003', '4', 'Предложите продукт 4', 2);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000004', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000005', '6', 'Предложите продукт 6', 3);
INSERT INTO sandbox.campaign_00003 VALUES ('ЦА-КП04-0622', '000000000025', '6', 'Предложите продукт 6', 3);


--
-- TOC entry 3740 (class 0 OID 18519)
-- Dependencies: 217
-- Data for Name: stop_00001; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_00001 VALUES ('000000000000');


--
-- TOC entry 3769 (class 0 OID 25103)
-- Dependencies: 265
-- Data for Name: stop_target_11; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_11 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_11 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_11 VALUES ('000000000000');


--
-- TOC entry 3771 (class 0 OID 25124)
-- Dependencies: 267
-- Data for Name: stop_target_12; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_12 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_12 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_12 VALUES ('000000000000');


--
-- TOC entry 3773 (class 0 OID 25130)
-- Dependencies: 269
-- Data for Name: stop_target_13; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_13 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_13 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_13 VALUES ('000000000000');


--
-- TOC entry 3775 (class 0 OID 25136)
-- Dependencies: 271
-- Data for Name: stop_target_14; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_14 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_14 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_14 VALUES ('000000000000');


--
-- TOC entry 3776 (class 0 OID 25139)
-- Dependencies: 272
-- Data for Name: stop_target_14_flags; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_14_flags VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_14_flags VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO sandbox.stop_target_14_flags VALUES ('000000000000', 1, 1, 0, 0, 1, 0);


--
-- TOC entry 3778 (class 0 OID 25147)
-- Dependencies: 274
-- Data for Name: stop_target_15; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_15 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_15 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_15 VALUES ('000000000000');


--
-- TOC entry 3780 (class 0 OID 25159)
-- Dependencies: 276
-- Data for Name: stop_target_16; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_16 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_16 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_16 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_16 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_16 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_16 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_16 VALUES ('000000000005');


--
-- TOC entry 3782 (class 0 OID 25165)
-- Dependencies: 278
-- Data for Name: stop_target_17; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_17 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_17 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_17 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_17 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_17 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_17 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_17 VALUES ('000000000005');


--
-- TOC entry 3784 (class 0 OID 25171)
-- Dependencies: 280
-- Data for Name: stop_target_18; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_18 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_18 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_18 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_18 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_18 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_18 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_18 VALUES ('000000000005');


--
-- TOC entry 3787 (class 0 OID 25183)
-- Dependencies: 283
-- Data for Name: stop_target_19; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_19 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_19 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_19 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_19 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_19 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_19 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_19 VALUES ('000000000005');


--
-- TOC entry 3789 (class 0 OID 25189)
-- Dependencies: 285
-- Data for Name: stop_target_20; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_20 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_20 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_20 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_20 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_20 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_20 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_20 VALUES ('000000000005');


--
-- TOC entry 3793 (class 0 OID 25201)
-- Dependencies: 290
-- Data for Name: stop_target_21; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_21 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_21 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_21 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_21 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_21 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_21 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_21 VALUES ('000000000005');


--
-- TOC entry 3795 (class 0 OID 25225)
-- Dependencies: 294
-- Data for Name: stop_target_22; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_22 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_22 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_22 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_22 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_22 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_22 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_22 VALUES ('000000000005');


--
-- TOC entry 3800 (class 0 OID 25364)
-- Dependencies: 299
-- Data for Name: stop_target_23; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_23 VALUES ('000000000025');
INSERT INTO sandbox.stop_target_23 VALUES ('000000000004');
INSERT INTO sandbox.stop_target_23 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_23 VALUES ('000000000003');
INSERT INTO sandbox.stop_target_23 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_23 VALUES ('000000000000');
INSERT INTO sandbox.stop_target_23 VALUES ('000000000005');


--
-- TOC entry 3761 (class 0 OID 24938)
-- Dependencies: 254
-- Data for Name: stop_target_7; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_7 VALUES ('ЦА-КП03-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.stop_target_7 VALUES ('ЦА-КП03-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.stop_target_7 VALUES ('ЦА-КП03-0622', '000000000002', '3', 'Предложите продукт 3', 2);
INSERT INTO sandbox.stop_target_7 VALUES ('ЦА-КП04-0622', '000000000000', '4', 'Предложите продукт 4', 2);
INSERT INTO sandbox.stop_target_7 VALUES ('ЦА-КП04-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.stop_target_7 VALUES ('ЦА-КП04-0622', '000000000002', '6', 'Предложите продукт 6', 3);


--
-- TOC entry 3762 (class 0 OID 24943)
-- Dependencies: 255
-- Data for Name: stop_target_8; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_8 VALUES ('ЦА-КП03-0622', '000000000000', '1', 'Предложите продукт 1', 1);
INSERT INTO sandbox.stop_target_8 VALUES ('ЦА-КП03-0622', '000000000001', '2', 'Предложите продукт 2', 1);
INSERT INTO sandbox.stop_target_8 VALUES ('ЦА-КП03-0622', '000000000002', '3', 'Предложите продукт 3', 2);
INSERT INTO sandbox.stop_target_8 VALUES ('ЦА-КП04-0622', '000000000000', '4', 'Предложите продукт 4', 2);
INSERT INTO sandbox.stop_target_8 VALUES ('ЦА-КП04-0622', '000000000001', '5', 'Предложите продукт 5', 1);
INSERT INTO sandbox.stop_target_8 VALUES ('ЦА-КП04-0622', '000000000002', '6', 'Предложите продукт 6', 3);


--
-- TOC entry 3767 (class 0 OID 25093)
-- Dependencies: 262
-- Data for Name: stop_target_9; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_9 VALUES ('000000000002');
INSERT INTO sandbox.stop_target_9 VALUES ('000000000001');
INSERT INTO sandbox.stop_target_9 VALUES ('000000000000');


--
-- TOC entry 3779 (class 0 OID 25150)
-- Dependencies: 275
-- Data for Name: stop_target_flags_15; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_15 VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_15 VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO sandbox.stop_target_flags_15 VALUES ('000000000000', 1, 1, 0, 0, 1, 0);


--
-- TOC entry 3781 (class 0 OID 25162)
-- Dependencies: 277
-- Data for Name: stop_target_flags_16; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000025', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000004', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000003', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000000', 1, 1, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_16 VALUES ('000000000005', 1, 0, 1, 0, 0, 0);


--
-- TOC entry 3783 (class 0 OID 25168)
-- Dependencies: 279
-- Data for Name: stop_target_flags_17; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000025', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000004', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000003', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000000', 1, 1, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_17 VALUES ('000000000005', 1, 0, 1, 0, 0, 0);


--
-- TOC entry 3785 (class 0 OID 25174)
-- Dependencies: 281
-- Data for Name: stop_target_flags_18; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000025', 0, NULL, NULL, NULL, NULL, NULL);
INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000004', 1, 0, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000002', 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000003', 1, 0, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000000', 1, 1, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_18 VALUES ('000000000005', 1, 0, 1, 0, 1, 0);


--
-- TOC entry 3788 (class 0 OID 25186)
-- Dependencies: 284
-- Data for Name: stop_target_flags_19; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000025', 1, 0, 1, 0, 0, 0);
INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000004', 1, 0, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000002', 1, 1, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000003', 1, 0, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000001', 1, 0, 0, 0, 1, 1);
INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000000', 1, 1, 0, 0, 1, 0);
INSERT INTO sandbox.stop_target_flags_19 VALUES ('000000000005', 1, 0, 1, 0, 1, 0);


--
-- TOC entry 3790 (class 0 OID 25192)
-- Dependencies: 286
-- Data for Name: stop_target_flags_20; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_20 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3794 (class 0 OID 25204)
-- Dependencies: 291
-- Data for Name: stop_target_flags_21; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_21 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3796 (class 0 OID 25228)
-- Dependencies: 295
-- Data for Name: stop_target_flags_22; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_22 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3801 (class 0 OID 25367)
-- Dependencies: 300
-- Data for Name: stop_target_flags_23; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000025', 1, 0, 1, 0, 0, 0, 0);
INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000004', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000002', 1, 1, 0, 1, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000003', 1, 0, 0, 0, 1, 0, 0);
INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000001', 1, 0, 0, 0, 1, 1, 1);
INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000000', 1, 1, 0, 0, 1, 1, 0);
INSERT INTO sandbox.stop_target_flags_23 VALUES ('000000000005', 1, 0, 1, 0, 1, 0, 0);


--
-- TOC entry 3748 (class 0 OID 24711)
-- Dependencies: 240
-- Data for Name: test_table_3; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--



--
-- TOC entry 3764 (class 0 OID 24954)
-- Dependencies: 257
-- Data for Name: test_table_4; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.test_table_4 VALUES ('0000000000');


--
-- TOC entry 3765 (class 0 OID 25043)
-- Dependencies: 258
-- Data for Name: test_table_5; Type: TABLE DATA; Schema: sandbox; Owner: postgres
--

INSERT INTO sandbox.test_table_5 VALUES ('0000000000');


--
-- TOC entry 3832 (class 0 OID 0)
-- Dependencies: 288
-- Name: autocheck; Type: SEQUENCE SET; Schema: prom; Owner: postgres
--

SELECT pg_catalog.setval('prom.autocheck', 5, true);


--
-- TOC entry 3833 (class 0 OID 0)
-- Dependencies: 237
-- Name: stop_dict_stop_id_seq; Type: SEQUENCE SET; Schema: prom; Owner: postgres
--

SELECT pg_catalog.setval('prom.stop_dict_stop_id_seq', 31, true);


--
-- TOC entry 3834 (class 0 OID 0)
-- Dependencies: 252
-- Name: stop_target_dict_target_id_seq; Type: SEQUENCE SET; Schema: prom; Owner: postgres
--

SELECT pg_catalog.setval('prom.stop_target_dict_target_id_seq', 23, true);


--
-- TOC entry 3835 (class 0 OID 0)
-- Dependencies: 289
-- Name: autocheck; Type: SEQUENCE SET; Schema: sandbox; Owner: postgres
--

SELECT pg_catalog.setval('sandbox.autocheck', 91, true);


--
-- TOC entry 3577 (class 2606 OID 19141)
-- Name: stop_dict stop_cd_unique; Type: CONSTRAINT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_dict
    ADD CONSTRAINT stop_cd_unique UNIQUE (stop_cd);


--
-- TOC entry 3579 (class 2606 OID 19139)
-- Name: stop_dict stop_dict_pkey; Type: CONSTRAINT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_dict
    ADD CONSTRAINT stop_dict_pkey PRIMARY KEY (stop_id);


--
-- TOC entry 3581 (class 2606 OID 24919)
-- Name: stop_target_dict stop_target_dict_pkey; Type: CONSTRAINT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_target_dict
    ADD CONSTRAINT stop_target_dict_pkey PRIMARY KEY (target_id);


--
-- TOC entry 3583 (class 2606 OID 24921)
-- Name: stop_target_dict target_cd_unique; Type: CONSTRAINT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_target_dict
    ADD CONSTRAINT target_cd_unique UNIQUE (target_cd);


--
-- TOC entry 3584 (class 2606 OID 19145)
-- Name: stop_repository fk_stop_id; Type: FK CONSTRAINT; Schema: prom; Owner: postgres
--

ALTER TABLE ONLY prom.stop_repository
    ADD CONSTRAINT fk_stop_id FOREIGN KEY (stop_id) REFERENCES prom.stop_dict(stop_id);


--
-- TOC entry 3786 (class 0 OID 25177)
-- Dependencies: 282 3826
-- Name: stop_1; Type: MATERIALIZED VIEW DATA; Schema: prom; Owner: postgres
--

REFRESH MATERIALIZED VIEW prom.stop_1;


--
-- TOC entry 3756 (class 0 OID 24854)
-- Dependencies: 249 3826
-- Name: stop_20; Type: MATERIALIZED VIEW DATA; Schema: prom; Owner: postgres
--

REFRESH MATERIALIZED VIEW prom.stop_20;


--
-- TOC entry 3757 (class 0 OID 24884)
-- Dependencies: 250 3826
-- Name: stop_9; Type: MATERIALIZED VIEW DATA; Schema: prom; Owner: postgres
--

REFRESH MATERIALIZED VIEW prom.stop_9;


--
-- TOC entry 3758 (class 0 OID 24891)
-- Dependencies: 251 3826
-- Name: test_mv_1; Type: MATERIALIZED VIEW DATA; Schema: sandbox; Owner: postgres
--

REFRESH MATERIALIZED VIEW sandbox.test_mv_1;


--
-- TOC entry 3763 (class 0 OID 24948)
-- Dependencies: 256 3826
-- Name: test_mv_2; Type: MATERIALIZED VIEW DATA; Schema: sandbox; Owner: postgres
--

REFRESH MATERIALIZED VIEW sandbox.test_mv_2;


--
-- TOC entry 3749 (class 0 OID 24716)
-- Dependencies: 241 3826
-- Name: test_table_1; Type: MATERIALIZED VIEW DATA; Schema: sandbox; Owner: postgres
--

REFRESH MATERIALIZED VIEW sandbox.test_table_1;


--
-- TOC entry 3750 (class 0 OID 24722)
-- Dependencies: 242 3826
-- Name: test_table_2; Type: MATERIALIZED VIEW DATA; Schema: sandbox; Owner: postgres
--

REFRESH MATERIALIZED VIEW sandbox.test_table_2;


-- Completed on 2022-07-06 19:59:07

--
-- PostgreSQL database dump complete
--

