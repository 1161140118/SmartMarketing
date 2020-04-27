package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
 * 用户语音交往圈月表:
 * 1. 数据清洗
 *    1. 过滤非手机号
 *    2. 过滤对端非哈尔滨
 *    3. 过滤本机到本机
 *    4. 关联语音信息统计月表，过滤离群点手机号记录
 * 2. 数据汇总
 *    1. 各种通话时长描述
 *    2. 各种通话次数描述
 *    3. 通话频繁度描述
 */

object SocialGsmCircleInfoM {

  def main(args: Array[String]): Unit = {
    val month = args(0)

    println(s"get args: $month .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances", "8")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "32G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.social_gsm_circle_info_m (
        |  userid string  comment "拨打方号码",
        |  opp_userid string  comment "接听方号码",
        |  calling_dur  int,
        |  calling_cnt  int,
        |  called_dur   int,
        |  called_cnt	  int,
        |  busy_dur   int,
        |  busy_cnt	  int,
        |  idle_dur	  int,
        |  idle_cnt	  int,
        |  call_days  int comment "通话天数",
        |  call_days_span int comment "首末通话日期跨度",
        |  call_dens  double  comment "通话天数稠密度 = 通话天数/日期跨度"
        |)
        |COMMENT ' 用户语音交往圈明细月表 '
        |PARTITIONED BY (
        |    `part_month` string COMMENT '数据月份分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val valid = ssc.sql(
      s"""
         |select userid as valid_userid from suyanli.social_gsm_stat_m
         |where part_month = '$month' and part_valid = 'true'
         |""".stripMargin)

    val my_date_diff = (pre: String, post:String) => pre.substring(6,8).toInt - post.substring(6,8).toInt + 1
    ssc.udf.register("my_date_diff", my_date_diff)

    val circle = ssc.sql(
      s"""
         |select
         |  serv_no as userid,
         |  opp_serv_no as opp_userid,
         |  sum(calling_dur) as calling_dur,
         |  sum(called_dur) as called_dur,
         |  sum(calling_cnt) as calling_cnt,
         |  sum(called_cnt) as called_cnt,
         |  sum(busy_dur) as busy_dur,
         |  sum(busy_call_cnt) as busy_cnt,
         |  sum(idle_dur) as idle_dur,
         |  sum(idle_call_cnt) as idle_cnt,
         |  max(call_days) as call_days,
         |  my_date_diff( max(last_call_date), min(first_call_date) ) as call_days_span
         |from
         |  (
         |    select * from suyanli.ti_ur_gsm_circle_m
         |    where part_month = '$month'
         |      and length(serv_no) = 11
         |      and length(opp_serv_no) = 11
         |      and serv_no between '1' and '2'
         |      and opp_serv_no between '1' and '2'
         |      and ( opp_region_code = '451' or opp_region_code > 'I' )
         |      and serv_no != opp_serv_no
         |  ) a
         |group by serv_no, opp_serv_no
         |""".stripMargin)
      .withColumn("call_dens", $"call_days"/$"call_days_span")
      .join(valid, $"userid" === valid("valid_userid")).drop("valid_userid")
      .join(valid, $"opp_userid" === valid("valid_userid")).drop("valid_userid")

    circle.withColumn("part_month",lit(month))
        .select("userid", "opp_userid",
          "calling_dur", "calling_cnt", "called_dur", "called_cnt",
          "busy_dur", "busy_cnt", "idle_dur", "idle_cnt",
          "call_days", "call_days_span", "call_dens", "part_month"
        )
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_month").insertInto("suyanli.social_gsm_circle_info_m")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }
}