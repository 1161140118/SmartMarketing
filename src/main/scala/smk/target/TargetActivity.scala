package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import utils.DateUtils

/**
 * 目标客户活跃度：
 * 1. 当日到访商场基站数，区分工作日/周末
 * 2. 周内日均到访商场基站数
 * 3. 月内日均到访商场基站数
 *
 * 依赖：
 * 1. mall_target_portrait 至少有前一周数据，用于过滤目标客户
 * 2. mall_target_trace_info  至少有到当日的数据，用于计算活跃度
 */
object TargetActivity {

  def main(args: Array[String]): Unit = {
    val date = args(0)
    val month = date.substring(0,6)
    val pre_date = DateUtils.dateAddAndFormat(date, -6)
    val day_type = if (DateUtils.isWeekend(date)) "weekend" else "weekday"

    println(s"get args: $date .")

    val conf = new SparkConf()
      .set("spark.executor.instances", "4")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "4G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val t1 = System.currentTimeMillis()


    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.mall_target_act_d (
        |  userid  string  comment "手机号",
        |  act_d  int,
        |  act_w  double,
        |  act_m  double
        |)
        |COMMENT ' 目标客户活跃度 '
        |PARTITIONED BY (
        |    `part_date` string COMMENT '数据日期分区 <partition field>',
        |    `part_dtype` string COMMENT '数据日期类型分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val t = ssc.sql(
      s"""
         |select distinct userid
         |from suyanli.mall_target_portrait
         |where ( start_date <= '$pre_date' and end_date >= '$pre_date' )
         |   or ( start_date <= '$date' and end_date >= '$date' )
         |""".stripMargin)

    val act = ssc.sql(
      s"""
         |select userid, count(*) as act_d, part_date
         |from suyanli.mall_target_trace_info
         |where part_date like '$month%'
         |group by userid, part_date
         |""".stripMargin).join(t,Seq("userid"))

//    val act = ssc.sql(
//      s"""
//         |select userid, count(distinct baseid) as act_d, part_date
//         |from suyanli.s1mm_trace
//         |where part_date like '$month%'
//         |group by userid, part_date
//         |""".stripMargin).join(t,Seq("userid"))

    val today = act.where(s" part_date='$date'")
    val week = act.where(s" part_date between '$pre_date' and '$date' ").groupBy("userid").agg( avg("act_d").as("act_w") )
    val monthly = act.where(s" part_date <= '$date' ").groupBy("userid").agg( avg("act_d").as("act_m") )

    val res = today.join(week,Seq("userid")).join(monthly, Seq("userid"))
      .selectExpr("userid","act_d","act_w","act_m",s" '$date' as part_date ", s" '$day_type' as part_dtype")

    res.write.mode("overwrite").format("parquet").partitionBy("part_date","part_dtype")
      .insertInto("suyanli.mall_target_act_d")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")
  }
}
