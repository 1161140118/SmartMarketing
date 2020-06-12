# 结合localname， 缩减目标客户规模， 提取目标客户信息
1. 根据localname中’商圈‘坐标信息，筛选到达过商圈的用户
   即获得到访问商圈这些商圈的顾客数据，也可辅助购物意图识别。
   购物中心共254所，人工去杂提纯后剩余212所，主要城区筛选剩余152所.
   
   主要城区范围：
   1. 江北松北城区：lng[126.457, 126.565],lat[45.795, 45.837]
   2. 江北师大城区：lng[126.550, 126.605],lat[45.860, 45.895]  // 126.549735,45.860136  126.604838,45.894853
   3. 江南中心城区：lng[126.522, 126.707],lat[45.691, 45.801]  // 126.522612,45.691439 126.706976,45.800928

   规则：
   1. 剔除结束时间在09点以前，和开始时间22点以后的记录
   2. 剔除持续时间低于30s的记录
   3. 通过与有效基站连接，标记商场
   
2. 计算用户访问浏览信息
   1. 对每个基站，根据地理坐标，绑定半径100米距离内的最近的商场，成功绑定的这部分基站作为有效基站
      限制每个商场附近最多绑定30个最近的基站（outlets附近共25个）
      注：1经度大约77.521km(北纬45.8度附近), 纬度1度大约111.195km，
   2. 用户连接有效基站即达到商场附近，连接记录标定商场名称
   3. 对每条用户访问记录，标定商场名
   4. 对每个 用户-商场 聚集，计算持续时间，开始、结束时间
      要求：
      1. 开始结束时间跨度超过20min，不超过360min
      2. 连接时间和超过10min
   5. 对每个用户聚集，标定当天浏览商场列表，并count，平局持续时间
      注：以7日为周期计算，周期去杂，噪声按(userid,mall,first_date)存入黑名单
   6. 对每个用户每周，标定周浏览商场列表，count，及访问次数最高的商场和访问次数



# 用户周汇总

``` sql


CREATE TABLE if not exists `mall_target_visit_weekly`(
   userid  string,
   visit_cnt  int  comment "访问次数，有记录的天数",
   start_min  string  comment "最早开始时间",
   end_max    string  comment "最晚结束时间",
   timespan_day_avg  int  comment "时间跨度每日平均值 min",
   timespan_mall_avg  int  comment "时间跨度每商场平均值 min",
   dura_day_avg  int  comment "持续时间每日平均值 min",
   dura_mall_avg  int  comment "持续时间每商场平均值 min",
   mall_cnt  int  comment  "访问不同商场个数",
   mall_list  map<string,int>  comment    "各商场访问次数",
   area_center_cnt   int,
   area_songbei_cnt  int,
   area_shida_cnt    int
)
COMMENT '用户周访问汇总，汇总用户周期内访问商场情况，通常周期为一周时间'
PARTITIONED BY (
    `start_date` string COMMENT '数据开始日期 <partition field>',
    `end_date`  string COMMENT '数据结束日期 <partition field>'
)
stored as parquet;




```


# 用户日访问汇总
smk.target.TargetVisitDaily

``` sql
-- hive: create table mall_target_visit_daily
--- truncate table mall_target_visit_daily;
--- drop table mall_target_visit_daily;

CREATE TABLE if not exists `mall_target_visit_daily`(
   userid   string,
   start_time  string comment "当日最早开始时间",
   end_time    string comment "当日最晚结束时间",
   timespan_total int comment "最早到最晚时间跨度/min",
   timespan_avg   int comment "平均每段时间跨度/min",
   dura_sum_sum   int comment "加和每段持续时间/min",
   dura_sum_avg   int comment "平均每段持续时间/min",
   mall_cnt    int comment "访问商场次数",
   mall_list   Array<string> comment "访问商场列表",
   area_center_cnt   int comment "区域’中心‘访问次数",
   area_songbei_cnt  int comment "区域’松北‘访问次数",
   area_shida_cnt    int comment "区域’师大‘访问次数",
   level int
)
COMMENT '用户日访问汇总，汇总用户当天访问商场情况'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>'
)
stored as parquet;

/*
show partitions mall_target_visit_daily;
select * from mall_target_visit_daily limit 10;
start_date  int  comment, count(*) from mall_target_visit_daily group by part_date;
start_date  int  comment, count(distinct userid) from mall_target_visit_daily group by part_date;
*/

```

# 顾客-商场 黑名单

``` sql
CREATE TABLE if not exists `mall_blacklist`(
string  int  comment,
string  int  comment,
   cnt      int      comment "研究周期内访问次数"
)
COMMENT '用户-商场 黑名单：访问次数较多c'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>'
)
stored as parquet;



```






# 用户-商场 聚集信息统计
smk.target.TargetTraceStat
计算当天，汇总用户在某商场产生的所有记录

*1.1当天共*

``` sql
-- hive: create table mall_target_trace_stat
level  int  comment, part_date
--- truncate table mall_target_trace_stat;
--- drop table mall_target_trace_stat;

CREATE TABLE if not exists `mall_target_trace_stat`(
string  int  comment,
string  int  comment,
   `timespan`  int    comment "从最早至最晚持续时间，单位分钟",
   `dura_sum_min` int   comment "加和每次连接持续时间，单位分钟",
   `dura_avg_min` int   comment "平均每次连接持续时间，单位分钟",
   `dura_avg_sec` int   comment "平均每次连接持续时间，单位秒",
   `enter` string comment "当天最早开始时间",
   `leave` string comment "当天最晚结束时间",
   `area`  string comment  "mall所属区域", 
   `level` int
)
COMMENT '用户-商场 聚集信息统计，汇总用户在某商场产生的所有记录'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>'
)
stored as parquet;

/*
show partitions mall_target_trace_stat;
select * from mall_target_trace_stat limit 10;
start_date  int  comment, count(*) from mall_target_trace_stat group by part_date;
start_date  int  comment, count(distinct userid) from mall_target_trace_stat group by part_date;
*/


```


# 目标用户访问商家标注
smk.target.TargetTraceInfo

*1.1当天共265w条记录*

分区表建表语句
``` sql
-- hive: create table mall_target_trace_info
level  int  comment, part_date
--- truncate table mall_target_trace_info;
--- drop table mall_target_trace_info;

CREATE TABLE if not exists `mall_target_trace_info`(
string  int  comment,
   `dura_sec`  int  comment  "连接持续时间，单位秒", 
   `mall`  string  comment  "商家/商圈名称", 
   `area`  string  comment  "mall所属区域", 
string  int  comment, 
string  int  comment,
   `level`  int
)
COMMENT '目标用户轨迹信息：访问主要区域mall的用户连接记录'
PARTITIONED BY (
   `part_date` string COMMENT '数据开始日期 <partition field>'
)
stored as parquet;

/*
show partitions mall_target_trace_info;
select * from mall_target_trace_info limit 10;
start_date  int  comment, count(*) from mall_target_trace_info group by part_date;
start_date  int  comment, count(distinct userid) from mall_target_trace_info group by part_date;
*/

```

# 枫叶小镇基站距离计算
``` scala

def cal(l1:Double, l2:Double, lng:Double = 126.463625, lat:Double = 45.821972 ) = {
    val rlng = (lng-l1)*77.521
    val rlat = (lat-l2)*111.195
    println( rlng, rlat )
    Math.sqrt( rlng*rlng + rlat*rlat )
}

126.463625,45.821972
val lng = 126.463625
val lat = 45.821972

126.466204,45.822727
126.466114,45.822818
126.461232,45.822759
126.458897,45.821937

scala> cal(126.466204,45.822727)
(-0.19992665900088633,-0.08395222499978458)
res8: Double = 0.21683783125107836

scala> cal(126.458897, 45.821937)
(0.3665192880000051,0.00389182500044523)
res7: Double = 0.366539949770642


```

# 主要基站数据过滤
``` scala

case class Base(baseid:String, lng:Float, lat:Float, city:String)
val base = sc.textFile("hdfs:///suyan/dm_cell_info.csv").map( _.split(","))
.map(x =>
   try
      Base(x(14), x(12).toFloat, x(13).toFloat, x(7))
   catch {
      case e: Exception => Base("",0,0,"")
   }
)
.toDF.distinct.where("baseid != '' ")
base.write.saveAsTable("suyanli.dm_cell_info")

// smk.target.MallCellMatch

```



# 主要商家信息上传
localname_main_mall 产生于 localname/mall_filter 

upload localname_main_mall.csv
``` scala
// hdfs dfs -put ~/chenzhihao/localname_main_mall.csv /suyan/
//                0            1              2             3            4          5         6           7              8          
case class Mall(store:String, labels:String, addr:String, lng:Float, lat:Float, lnum:Int, label2:String, label3:String, area:String)
val mall = sc.textFile("hdfs:///suyan/localname_main_mall.csv").map( _.split(",")).map( x=> Mall( x(0),x(1),x(2),x(3).toFloat,x(4).toFloat,x(5).toInt,x(6),x(7),x(8) ) ).toDF
mall.saveAsTable("suyanli.localname_main_mall")
```

