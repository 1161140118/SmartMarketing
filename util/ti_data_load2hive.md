# 移动ti数据，导入hive

# 家庭成员基本信息日表
ti_fb_member_d
20200101-20200201 done.

由于每个文件约5M，不分区存储

``` sql

CREATE TABLE if not exists `ti_fb_member_d`(
`stat_date`  string  comment "统计日期",
`family_id`  string  comment "家庭标识",  
`city_code`  string  comment "地市编码",  
`county_code`  string  comment "区县编码",  
`user_id`  string  comment "用户标识",  
`serv_no`  string  comment "手机号码",  
`role_type`  string  comment "角色类型",  
`offnet_flag`  string  comment "离网标志",  
`offnet_date`  date  comment "离网日期",  
`comm_flag`  string  comment "通信标志",  
`family_create_date`  date  comment "家庭成立时间",  
`family_dismis_date`  date  comment "家庭解散时间"
)
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>'
)
stored as parquet;

```
``` scala
// 数据量不大，用spark-shell导数据

case class Member(
    stat_date:String,
    family_id:String,
    city_code:String,
    county_code:String,
    user_id:String,
    serv_no:String,
    role_type:String,
    offnet_flag:String,
    offnet_date:String,
    comm_flag:String,
    family_create_date:String,
    family_dismis_date:String
)
sc.textFile("hdfs:///suyan/ti_fb_member_d_202004*").map( _.split("\\|",12) ).map( x => Member( x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)) ).toDF.withColumn("part_date",$"stat_date").write.mode("overwrite").format("parquet").partitionBy("part_date").insertInto("suyanli.ti_fb_member_d")

```









# 用户语音按基站日表
ti_ub_gsm_bs_d
20200101-20200201

spark job: smk.dataops.LoadTiUbGsmBsD

``` sql

CREATE TABLE if not exists `ti_ub_gsm_bs_d`(
`stat_date`     string  comment "统计日期",
`call_period`   string  comment "通话时段",
`serv_no`   string  comment "手机号码",
`lac`   string  comment "位置码",
`ci`    string  comment "小区码",
`bs_id` string  comment "基站标识",
`total_call_dur`    bigint  comment "总通话时长",
`calling_dur`   bigint  comment "主叫通话时长",
`called_dur`    bigint  comment "被叫通话时长",
`call_cnt`  bigint  comment "通话次数",
`calling_cnt`   bigint  comment "主叫通话次数",
`called_cnt`    bigint  comment "被叫通话次数",
`one_min_calling_cnt`   bigint  comment "1分钟以下主叫通话次数",
`local_call_cnt`    bigint  comment "本地通话次数",
`toll_call_cnt`     bigint  comment "长途通话次数",
`roam_call_cnt`     bigint  comment "漫游通话次数"
)
COMMENT '用户语音按基站日表'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>'
)
stored as parquet;
```
``` scala
case class Member(
    stat_date:String,
    call_period:String,
    serv_no:String,
    lac:String,
    ci:String,
    bs_id:String,
    total_call_dur:Long,
    calling_dur:Long,
    called_dur:Long,
    call_cnt:Long,
    calling_cnt:Long,
    called_cnt:Long,
    one_min_calling_cnt:Long,
    local_call_cnt:Long,
    toll_call_cnt:Long,
    roam_call_cnt:Long
)
sc.textFile("hdfs:///suyan/ti_ub_gsm_bs_d*").map( _.split("\\|",16) ).map( x => Member( x(0), x(1), x(2), x(3), x(4), x(5), x(6).toLong, x(7).toLong, x(8).toLong, x(9).toLong, x(10).toLong, x(11).toLong, x(13).toLong, x(14).toLong, x(12).toLong, x(15).toLong )).toDF.withColumn("part_date",$"stat_date").write.mode("overwrite").format("parquet").partitionBy("part_date").insertInto("suyanli.ti_ub_gsm_bs_d")
```









# 用户基本信息月表
ti_ue_basic_info_m
202001 done.

``` sql
CREATE TABLE if not exists `ti_ue_basic_info_m`(
`stat_mon`  string  comment "统计月份",
`user_id`  bigint  comment "用户标识",
`cust_id`  bigint  comment "客户标识",
`serv_no`  string  comment "手机号码",
`city_code`  string  comment "地市编码",
`county_code`  string  comment "区县编码",
`cust_name`  string  comment "客户姓名",
`cust_gender`  string  comment "客户性别",
`cust_age`  int  comment "客户年龄",
`cust_birth_date`  string  comment "客户出生日期"
)
COMMENT '用户基本信息月表'
PARTITIONED BY (
    `part_month` string COMMENT '数据月份分区 <partition field>'
)
stored as parquet;
```
``` scala
case class BasicInfo(
    stat_mon:String,
    user_id:Long,
    cust_id:Long,
    serv_no:String,
    city_code:String,
    county_code:String,
    cust_name:String,
    cust_gender:String,
    cust_age:Int,
    cust_birth_date:String
)
sc.textFile("hdfs:///suyan/ti_ue_basic_info_m_202003.txt").map( _.split("\\|",10) ).map( x => BasicInfo( x(0), x(1).toLong, x(2).toLong, x(3), x(4), x(5), x(6), x(7), x(8).toInt, x(9) )).toDF.withColumn("part_month",$"stat_mon").write.mode("overwrite").format("parquet").partitionBy("part_month").insertInto("suyanli.ti_ue_basic_info_m")
```









# 个人客户基本信息日表
ti_ib_indiv_base_d
20200101

``` sql

CREATE TABLE if not exists `ti_ib_indiv_base_d`(
`stat_date`  string  comment "统计日期",
`cust_id`   string  comment "客户标识",
`cust_name`  string  comment "客户名称",
`city_code`  string  comment "地市编码",
`county_code`  string  comment "区县编码",
`cust_addr`  string  comment "居住地址",
`unit_name`  string  comment "单位名称",
`birth_month`  string  comment "出生年月",
`nation`  string  comment "民族",
`language`  string  comment "语言",
`nationality`  string  comment "国籍",
`gender`  string  comment "性别",
`age`  int  comment "年龄"
)
COMMENT '个人客户基本信息日表'
PARTITIONED BY (
    `part_date` string COMMENT '数据日期分区 <partition field>'
)
stored as parquet;

```
``` scala
case class Indiv(
    stat_date:String,
    cust_id:String,
    cust_name:String,
    city_code:String,
    county_code:String,
    cust_addr:String,
    unit_name:String,
    birth_month:String,
    nation:String,
    language:String,
    nationality:String,
    gender:String,
    age:Int
)

sc.textFile("hdfs:///suyan/ti_ib_indiv_base_d_20200101.txt")
    .map( r => {
        val x = r.split("\\|", 20)
        if (x.length == 13) {
            Indiv(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12).toInt)
        } else {
            val l = x.length
            Indiv(x(0), x(1), x(2), x(3), x(4), x.slice(5, l - 7).mkString("/"), x(l - 7), x(l - 6), x(l - 5), x(l - 4), x(l - 3), x(l - 2), x(l - 1).toInt)
        }
    })
    .toDF().withColumn("part_date",$"stat_date")
    .write.mode("overwrite").format("parquet")
    .partitionBy("part_date").insertInto("suyanli.ti_ib_indiv_base_d")

```









# 用户语音按基站月表
ti_ub_gsm_bs_m
202001
201912

``` sql
CREATE TABLE if not exists `ti_ub_gsm_bs_m`(
    `stat_mon` string comment "统计月份",
    `call_period` string comment "通话时段",
    `serv_no` string comment "服务号码",
    `home_region_code` string comment "归属地",
    `lac` string comment "位置码",
    `ci` string comment "小区码",
    `bs_id` string comment "基站标识",
    `call_dur` bigint comment "总通话时长",
    `calling_dur` bigint comment "主叫通话时长",
    `called_dur` bigint comment "被叫通话时长",
    `call_cnt` bigint comment "通话次数",
    `calling_cnt` bigint comment "主叫通话次数",
    `called_cnt` bigint comment "被叫通话次数",
    `one_min_calling_cnt` bigint comment "1分钟以下主叫通话次数",
    `local_call_cnt` bigint comment "本地通话次数",
    `toll_call_cnt` bigint comment "长途通话次数",
    `roam_call_cnt` bigint comment "漫游通话次数",
    `weekday_call_cnt` bigint comment "工作日通话次数",
    `weekday_calling_cnt` bigint comment "工作日主叫通话次数",
    `weekday_called_cnt` bigint comment "工作日被叫通话次数",
    `weekend_call_cnt` bigint comment "周末通话次数",
    `weekend_calling_cnt` bigint comment "周末主叫通话次数",
    `weekend_called_cnt` bigint comment "周末被叫通话次数",
    `weekday_call_dur` bigint comment "工作日通话时长",
    `weekend_call_dur` bigint comment "周末通话时长",
    `weekday_busycall_cnt` bigint comment "工作日忙时通话次数"
)
COMMENT '用户语音按基站月表'
PARTITIONED BY (
    `part_month` string COMMENT '数据月份分区 <partition field>'
)
stored as parquet;
```
``` scala

class GsmInfo ( 
    stat_mon:String,
    call_period:String,
    serv_no:String,
    home_region_code:String,
    lac:String,
    ci:String,
    bs_id:String,
    call_dur:Long,
    calling_dur:Long,
    called_dur:Long,
    call_cnt:Long,
    calling_cnt:Long,
    called_cnt:Long,
    one_min_calling_cnt:Long,
    local_call_cnt:Long,
    toll_call_cnt:Long,
    roam_call_cnt:Long,
    weekday_call_cnt:Long,
    weekday_calling_cnt:Long,
    weekday_called_cnt:Long,
    weekend_call_cnt:Long,
    weekend_calling_cnt:Long,
    weekend_called_cnt:Long,
    weekday_call_dur:Long,
    weekend_call_dur:Long,
    weekday_busycall_cnt:Long
) extends Product() with Serializable{
    def productElement(n: Int) = n match {
        case 0 => stat_mon 
        case 1 => call_period 
        case 2 => serv_no 
        case 3 => home_region_code 
        case 4 => lac 
        case 5 => ci 
        case 6 => bs_id 
        case 7 => call_dur 
        case 8 => calling_dur 
        case 9 => called_dur 
        case 10 => call_cnt 
        case 11 => calling_cnt 
        case 12 => called_cnt 
        case 13 => one_min_calling_cnt 
        case 14 => local_call_cnt 
        case 15 => toll_call_cnt 
        case 16 => roam_call_cnt 
        case 17 => weekday_call_cnt 
        case 18 => weekday_calling_cnt 
        case 19 => weekday_called_cnt 
        case 20 => weekend_call_cnt 
        case 21 => weekend_calling_cnt 
        case 22 => weekend_called_cnt 
        case 23 => weekday_call_dur 
        case 24 => weekend_call_dur 
        case 25 => weekday_busycall_cnt 
    }
    def canEqual(that: Any) = that.isInstanceOf[GsmInfo]
    def productArity = 26
}

sc.textFile("hdfs:///suyan/ti_ub_gsm_bs_m_202003.txt").map( _.split("\\|",26) ).map( x => new GsmInfo( x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7).toLong,x(8).toLong,x(9).toLong,x(10).toLong,x(11).toLong,x(12).toLong,x(13).toLong,x(14).toLong,x(15).toLong,x(16).toLong,x(17).toLong,x(18).toLong,x(19).toLong,x(20).toLong,x(21).toLong,x(22).toLong,x(23).toLong,x(24).toLong,x(25).toLong )).toDF.withColumn("part_month",$"stat_mon").write.mode("overwrite").format("parquet").partitionBy("part_month").insertInto("suyanli.ti_ub_gsm_bs_m")

```









# 用户语音交往圈月表
ti_ur_gsm_circle_m
202001
201912

``` sql
CREATE TABLE if not exists `ti_ur_gsm_circle_m`(
    `stat_mon` string comment "统计月份",
    `serv_no` string comment "手机号码",
    `opp_serv_no` string comment "对端号码",
    `opp_type` string comment "对端类型",
    `opp_region_code` string comment "对端归属地",
    `call_dur` bigint comment "通话时长(秒)",
    `calling_dur` bigint comment "通话时长(秒)(主叫)",
    `called_dur` bigint comment "通话时长(秒)(被叫)",
    `call_cnt` bigint comment "通话次数",
    `calling_cnt` bigint comment "通话次数(主叫)",
    `called_cnt` bigint comment "通话次数(被叫)",
    `busy_call_cnt` bigint comment "忙时通话次数",
    `idle_call_cnt` bigint comment "闲时通话次数",
    `busy_dur` bigint comment "忙时通话时长",
    `idle_dur` bigint comment "闲时通话时长",
    `weekday_sum_call_cnt` bigint comment "工作日通话次数",
    `weekday_work_sum_call_cnt` bigint comment "工作日上班时间通话次数",
    `weekday_offwork_sum_call_cnt` bigint comment "工作日非上班时间通话次数",
    `weekend_sum_call_cnt` bigint comment "周末通话次数",
    `weekday_sum_call_dur` bigint comment "工作日通话时长",
    `weekend_sum_call_dur` bigint comment "周末通话时长",
    `call_days` bigint comment "通话天数",
    `first_call_date` string comment "末次通话时间",
    `last_call_date` string comment "首次通话时间"
)
COMMENT '用户语音交往圈月表'
PARTITIONED BY (
    `part_month` string COMMENT '数据月份分区 <partition field>'
)
stored as parquet;
```
``` scala
class GsmCircle ( 
    stat_mon:String,
    serv_no:String,
    opp_serv_no:String,
    opp_type:String,
    opp_region_code:String,
    call_dur:Long,
    calling_dur:Long,
    called_dur:Long,
    call_cnt:Long,
    calling_cnt:Long,
    called_cnt:Long,
    busy_call_cnt:Long,
    idle_call_cnt:Long,
    busy_dur:Long,
    idle_dur:Long,
    weekday_sum_call_cnt:Long,
    weekday_work_sum_call_cnt:Long,
    weekday_offwork_sum_call_cnt:Long,
    weekend_sum_call_cnt:Long,
    weekday_sum_call_dur:Long,
    weekend_sum_call_dur:Long,
    call_days:Long,
    first_call_date:String,
    last_call_date:String
) extends Product() with Serializable{
    def productElement(n: Int) = n match {
        case 0 => stat_mon 
        case 1 => serv_no 
        case 2 => opp_serv_no 
        case 3 => opp_type 
        case 4 => opp_region_code 
        case 5 => call_dur 
        case 6 => calling_dur 
        case 7 => called_dur 
        case 8 => call_cnt 
        case 9 => calling_cnt 
        case 10 => called_cnt 
        case 11 => busy_call_cnt 
        case 12 => idle_call_cnt 
        case 13 => busy_dur 
        case 14 => idle_dur 
        case 15 => weekday_sum_call_cnt 
        case 16 => weekday_work_sum_call_cnt 
        case 17 => weekday_offwork_sum_call_cnt 
        case 18 => weekend_sum_call_cnt 
        case 19 => weekday_sum_call_dur 
        case 20 => weekend_sum_call_dur 
        case 21 => call_days 
        case 22 => first_call_date 
        case 23 => last_call_date 
    }
    def canEqual(that: Any) = that.isInstanceOf[GsmCircle]
    def productArity = 24
}

sc.textFile("hdfs:///suyan/ti_ur_gsm_circle_m_202002.txt").map( _.split("\\|",24) ).map( x => new GsmCircle( x(0), x(1), x(2), x(3), x(4), x(5).toLong,  x(6).toLong,  x(7).toLong, x(8).toLong, x(9).toLong, x(10).toLong, x(11).toLong, x(12).toLong, x(13).toLong, x(14).toLong, x(15).toLong, x(16).toLong, x(17).toLong, x(18).toLong, x(19).toLong, x(20).toLong, x(21).toLong, x(22), x(23) )).toDF.withColumn("part_month",$"stat_mon").write.mode("overwrite").format("parquet").partitionBy("part_month").insertInto("suyanli.ti_ur_gsm_circle_m")
```









# 点对点短信结算月表
ti_ub_p2p_sms_settle_m
202001
201912

``` sql
CREATE TABLE if not exists `ti_ub_p2p_sms_settle_m`(
    `statis_date` string comment "统计日期",
    `user_type` string comment "用户类型",
    `send_serv_no` string comment "发送方号码",
    `home_city_code` string comment "发送方归属区号",
    `reci_serv_no` string comment "接收方号码",
    `opp_city_code` string comment "接收方归属区号",
    `sms_count` bigint comment "短信条数"
)
COMMENT '用户语音按基站月表'
PARTITIONED BY (
    `part_month` string COMMENT '数据月份分区 <partition field>'
)
stored as parquet;
```
``` scala
case class Info(
    statis_date:String,
    user_type:String,
    send_serv_no:String,
    home_city_code:String,
    reci_serv_no:String,
    opp_city_code:String,
    sms_count:Long
)

sc.textFile("hdfs:///suyan/ti_ub_p2p_sms_settle_m_*.txt").map( _.split("\\|",7) ).map( x => Info( x(0), x(1), x(2), x(3), x(4), x(5), x(6).toLong)).toDF.withColumn("part_month",$"statis_date").write.mode("overwrite").format("parquet").partitionBy("part_month").insertInto("suyanli.ti_ub_p2p_sms_settle_m")

```



