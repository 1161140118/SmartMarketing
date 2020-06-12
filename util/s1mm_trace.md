# 轨迹处理

# 轨迹数据整理
``` sql
-- hive: create table 
-- day, iccid1, iccid2, userid, baseid, basein, startt, endt, level
CREATE TABLE `s1mm_trace`(
    `iccid1`    string,
    `iccid2`    string,
    `userid`    string,
    `baseid`    string,
    `basein`    string,
    `startt`    bigint,
    `endt`  bigint,
    `level` int
)
COMMENT 'trace data from s1mm tables'
PARTITIONED BY (
  `part_date` string COMMENT '数据分区 <partition field>'
)
stored as parquet;

show partitions s1mm_trace;

select part_date, count(*) from s1mm_trace where part_date>='20200203' group by part_date;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


-- 导数据
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200101;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200102;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200103;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200104;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200105;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200106;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200107;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200108;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200109;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200110;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200111;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200112;


insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200113;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200114;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200115;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200116;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200117;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200118;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200119;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200120;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200121;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200122;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200123;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200124;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200125;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200126;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200127;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200128;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200129;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200130;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200131;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200201;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200202;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200203;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200204;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200205;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200206;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200207;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200208;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200209;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200210;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200211;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200212;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200213;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200214;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200215;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200216;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200217;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200218;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200219;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200220;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200221;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200222;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200223;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200224;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200225;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200226;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200227;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200228;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200229;
insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200301;

insert overwrite table s1mm_trace PARTITION (part_date)
select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from s1mm_20200302;

```

# S1MME 
``` sql

CREATE external TABLE `s1mm_trace_tmp`(
    `day` string,
    `iccid1`    string,
    `iccid2`    string,
    `userid`    string,
    `baseid`    string,
    `basein`    string,
    `startt`    bigint,
    `endt`  bigint,
    `level` int
)
COMMENT 'tmp table fro trace data from s1mm tables'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
stored as textfile
LOCATION '/user/hive/warehouse/s1mm_trace_tmp';

load data inpath '/suyan/tb_signal_mme_d_201912*' into table s1mm_trace_tmp;


```


## hive 导出到 hdfs
``` sql

SET hive.exec.compress.output=false;
insert overwrite directory 'hdfs:///suyan/s1mm_20200116' 
row format delimited fields terminated by '|'
stored as textfile
select * from suyanli.s1mm_20200116;


-- 写出到 hdfs

SET mapreduce.output.fileoutputformat.compress.codec = gzip;

insert overwrite  directory '/user/finance/hive/warehouse/fdm_sor.db/t_tmp/'
select * from t_tmp;

```