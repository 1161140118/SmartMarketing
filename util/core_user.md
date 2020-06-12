# 核心客户抽取
1. 创建 hive:mall_cell_info，存储outlets的基站数据
2. 抽取 s1mm 轨迹数据 1.1-1.12 数据，过滤到访过outlets基站的用户，写入表 mall_user_trace_info
3. 对 mall_user_trace_info 中 user 聚集统计base切换次数、总持续时长、平局持续时长等信息, 作为 mall_user_trace_stat
4. 根据规则，过滤 mall_user_trace_info 中user，去除商场员工等特殊人群，剩余重点客户为vip，按周统计该周内总访问时间，访问次数
   写入表 mall_vip_info_week
5. **注意 1.25 - 3.8 期间停业, 1.25前参考营业时间为9-22， 3.9后为10:30-19:00**

# vip 客户发现及打分

噪声数据清除规则：
1. 根据记录时间剔除数据：
   1. 早9点之前剔除
   2. 持续超过10小时剔除
   3. 平均连接时间为0min(<60s)
2. 根据日期上下文，一周内连续4天(天数/2 向上取整）以上出现者，为商场员工或附近居民
3. 以当天持续时间长短作为衡量用户重要性依据之一

噪声数据清除规则：
1. 根据记录时间剔除数据：
   1. 9-22 点以外
   2. 持续超过10小时剔除
   3. 平均连接时间为0min(<60s)
   4. 基站连接个数为1
2. 根据日期上下文，一周内连续4天(天数/2 向上取整）以上出现者，为商场员工或附近居民

说明：
1. 以当天持续时间长短作为衡量用户重要性依据之一
2. 考虑到疫情影响 1.1-1.7, 1.8-1.14, 1.15-1.21 作为三个研究周期
3. 正常营业时间 9:00-22:00，3.9日起疫情影响营业时间 10:30-19:00
   
   
* 研究周期 1.1-1.7
    总计 102497 人次
    根据规则1，剩余 23995 人次
    根据规则2，剩余 20714 人次
    即 估计约 3000 人次 为商场员工记录
* 对于 1.1 单日(工作日）：
    总计 15128 人次
    根据规则1，剩余 4774 人次
    根据规则2，剩余 4383 人次
    即 估计约 400 人次 为商场员工记录
* 对于 1.5 单日(周日）：
    总计 17268 人次
    根据规则1，剩余 5042 人次
    根据规则2，剩余 4564 人次
    即 估计约 500 人次 为商场员工记录

``` scala
// mall_user_trace_stat: userid, dura, dura_sum, dura_avg, dura_avg_s, enter, leave, base_conn_cnt, base_uniq_cnt, level, part_date, part_mall
val ssc = sqlContext // shell中使用，jar中不用
import ssc.implicits._

   val mall = "outlets"
   val start = "20200101"
   val end = "20200107"
   val dateThreshold = 3

    val din = ssc.sql(
      s"""
         |select
         |  userid,
         |  count(*) as cnt,
         |  min(enter) as enter_min,
         |  max(leave) as leave_max,
         |  avg(dura) as timespan_avg,
         |  avg(dura_sum) as durasum_avg,
         |  sum(dura_sum) as durasum_sum,
         |  avg(base_conn_cnt) as base_conn_cnt_avg,
         |  avg(base_uniq_cnt) as base_uniq_cnt_avg,
         |  first(a.level) as level,
         |  '$start' as start_date,
         |  '$end' as end_date,
         |  '$mall' as part_mall
         |from
         |(
         |  select * from suyanli.mall_user_trace_stat
         |  where length(userid) = 11
         |    and enter between '09' and '22'
         |    and leave between '09' and '22'
         |    and dura < 600
         |    and dura_avg > 0
         |    and base_uniq_cnt > 1
         |    and part_mall = '$mall'
         |    and part_date between "$start" and "$end"
         |) a
         |group by userid
         |having count(*) <= '$dateThreshold'
       """.stripMargin)

    val d = ssc.sql(
      s"""
         |select * from
         |(
         |  select * from suyanli.mall_user_trace_stat
         |  where enter between '09' and '22'
         |    and leave between '09' and '22'
         |    and dura < 600
         |    and dura_avg > 0
         |    and base_uniq_cnt > 1
         |    and part_mall = '$mall'
         |    and part_date between "$start" and "$end"
         |) a
         |join
         |( select userid
         |  from suyanli.mall_user_trace_stat
         |  where part_date between "$start" and "$end"
         |  group by userid
         |  having count(*) <= 3
         |) b
         |on a.userid = b.userid
       """.stripMargin)

    val o = ssc.sql(
      s"""
         |  select * from suyanli.mall_user_trace_stat
         |  where enter between '09' and '22'
         |    and leave between '09' and '22'
         |    and dura < 600
         |    and dura_avg > 0
         |    and base_uniq_cnt > 1
         |    and part_mall = '$mall'
         |    and part_date between "$start" and "$end"
       """.stripMargin)

    ssc.sql(s""" select count(*) from suyanli.mall_user_trace_stat where part_date between "$start" and "$end" """)

```

分区表建表语句
``` sql
-- hive: create table 
-- userid, cnt, enter_min, leave_max, timespan_avg, durasum_avg, durasum_sum, base_conn_cnt_avg, base_uniq_cnt_avg, level
--- truncate table mall_vip_info_week;
--- drop table mall_vip_info_week;

CREATE TABLE if not exists `mall_vip_info_week`(
    `userid`    string,
    `cnt`   int comment "访问次数，有记录的天数",
    `enter_min` string comment "最早开始时间",
    `leave_max` string comment "最晚结束时间",
    `timespan_avg`  int comment "时间跨度平均值，avg(dura) /min",
    `durasum_avg`   int comment "连接时间和平均，avg(dura_sum) /min",
    `durasum_sum`   int comment "连接时间和总计，sum(dura_sum) /min",
    `base_conn_cnt_avg` int comment "基站切换次数均值",
    `base_uniq_cnt_avg` int comment "不同基站访问次数均值",
    `level` int
)
COMMENT 'vip用户周统计信息，vip用户为从stat表提纯的商场有效访客'
PARTITIONED BY (
    `start_date` string COMMENT '数据开始日期 <partition field>',
    `end_date`  string COMMENT '数据结束日期 <partition field>',
    `part_mall` string COMMENT 'user所在mall <partition field>'
)
stored as parquet;

/*
show partitions mall_vip_info_week;
select * from mall_vip_info_week limit 10;
select start_date, count(*) from mall_vip_info_week group by start_date;
select start_date, count(distinct userid) from mall_vip_info_week group by start_date;
*/


```


# outlets 用户访问数据统计
对 mall_user_trace_info 用户行为统计：
1. 提供去除商场员工等人群的数据参考
2. 提供对顾客进行评分，衡量忠实程度的指标参考

从 mall_user_trace_info 汇总每个顾客单日信息
``` scala
val ssc = sqlContext // shell中使用，jar中不用
import ssc.implicits._

val date = "20200101"
val stat = ssc.sql(
    s"""
    |select 
    |   userid, part_date, part_mall,
    |   ( max(endt) - min(startt) )/60000 as dura,
    |   sum( duration ) as dura_sum_s,
    |   avg( duration ) as dura_avg_s,
    |   from_unixtime( min( startt )/1000,'HH:mm:ss') as enter,
    |   from_unixtime( max( endt )/1000,'HH:mm:ss') as leave,
    |   count(*) as base_conn_cnt,
    |   count(distinct baseid) as base_uniq_cnt,
    |   first(level) as level
    |from 
    |   (select *, (endt-startt)/1000 as duration from suyanli.mall_user_trace_info where part_date>'$date') a
    |group by userid, part_date, part_mall
    """.stripMargin)


// 写入hive
ssc.sql("set hive.exec.dynamic.partition=true;")
ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
stat.selectExpr("userid", "dura", "dura_sum_s/60 as dura_sum", "dura_avg_s/60 as dura_avg", "dura_avg_s", "enter", "leave", "base_conn_cnt", "base_uniq_cnt", "level", "part_date", "part_mall").write.mode("overwrite").format("parquet").partitionBy("part_date","part_mall").insertInto("suyanli.mall_user_trace_stat")


```
分区表建表语句
``` sql
-- hive: create table 
-- mall_user_trace_stat: userid, dura, dura_sum, dura_avg, dura_avg_s, enter, leave, base_conn_cnt, base_uniq_cnt, level, part_date, part_mall
--- truncate table mall_user_trace_stat;
--- drop table mall_user_trace_stat;

CREATE TABLE if not exists `mall_user_trace_stat`(
    `userid`    string,
    `dura`      int    comment "从最早至最晚持续时间，单位分钟",
    `dura_sum`  int    comment "加和每次连接持续时间，单位分钟",
    `dura_avg`  int    comment "平均每次连接持续时间，单位分钟",
    `dura_avg_s` int   comment "平均每次连接持续时间，单位秒",
    `enter` string comment "当天最早开始时间",
    `leave` string comment "当天最晚结束时间",
    `base_conn_cnt` int comment "基站切换次数，即当天产生记录数",
    `base_uniq_cnt` int comment "连接过的不同基站数，描述移动范围",
    `level` int
)
COMMENT 'mall user trace data extracted from s1mm tables'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>',
    `part_mall` string COMMENT 'user所在mall <partition field>'
)
stored as parquet;

/*
show partitions mall_user_trace_stat;
select * from mall_user_trace_stat limit 10;
select part_date, count(*) from mall_user_trace_stat group by part_date;
select part_date, count(distinct userid) from mall_user_trace_stat group by part_date;
*/
```





# outlets 用户访问数据抽取
过滤出访问过outlets的用户，保留主要原始信息。
已过滤：1.1-3.2
``` scala
val ssc = sqlContext // shell中使用，jar中不用
import ssc.implicits._

val cell = ssc.sql("select distinct baseid from suyanli.mall_cell_info where mall='outlets' ") // "mall","baseid","lng","lat"
val trace = ssc.sql("select userid, baseid, startt, endt, level, part_date from suyanli.s1mm_trace where part_date between '20200203' and '20200209' ")
val outlets = trace.join(cell,Seq("baseid"))

ssc.sql("set hive.exec.dynamic.partition=true;")
ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
import org.apache.spark.sql.functions._
outlets.selectExpr("userid", "baseid", "startt", "endt", "level", "part_date", "'outlets' as part_mall").write.mode("overwrite").format("parquet").partitionBy("part_date","part_mall").insertInto("suyanli.mall_user_trace_info")


```
分区表建表语句
``` sql
-- hive: create table 
-- userid, baseid, startt, endt, level
CREATE TABLE `mall_user_trace_info`(
    `userid`    string,
    `baseid`    string,
    `startt`    bigint,
    `endt`  bigint,
    `level` int
)
COMMENT 'mall user trace data extracted from s1mm tables'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>',
    `part_mall` string COMMENT 'user 所在的 mall <partition field>'
)
stored as parquet;


/*
show partitions mall_user_trace_info;
select * from mall_user_trace_info limit 10;
select part_date, count(*) from mall_user_trace_info group by part_date;
select part_date, count(distinct userid) from mall_user_trace_info group by part_date;
*/

```






# 存储outlets（枫叶小镇奥特莱斯）基站数据到hive
``` scala
import sqlContext.implicits._
sc.parallelize( Seq(
    "4731139,126.466204,45.822727 ",
    "4731134,126.466114,45.822818 ",
    "4731135,126.466114,45.822818 ",
    "4731136,126.466114,45.822818 ",
    "473AD1,126.461232,45.822759  ",
    "473AD2,126.461232,45.822759  ",
    "473AD3,126.461232,45.822759  ",
    "473AD4,126.461232,45.822759  ",
    "476A41,126.461232,45.822759  ",
    "476A42,126.461232,45.822759  ",
    "4731131,126.466114,45.822818 ",
    "4731132,126.466114,45.822818 ",
    "4731133,126.466114,45.822818 ",
    "4768331,126.458897,45.821937 ",
    "4768332,126.458897,45.821937 ",
    "4768333,126.458897,45.821937 ",
    "CE8BAB1,126.466204,45.822727 ",
    "CE8BAB2,126.466204,45.822727 ",
    "CE8BBB1,126.458897,45.821937 ",
    "CE8BBB2,126.458897,45.821937 ",
    "CE8BBB3,126.458897,45.821937 ",
    "462E87DD,126.466204,45.822727",
    "462E87DE,126.466204,45.822727",
    "462E87DF,126.466204,45.822727",
    "462EAFB,126.461232,45.822759 "
)).map(_.split(",")).map( l => ("outlets",l(0),l(1).toDouble,l(2).toDouble))
    .toDF("mall","baseid","lng","lat")
    .write.mode(SaveMode.Append).saveAsTable("suyanli.mall_cell_info")
```