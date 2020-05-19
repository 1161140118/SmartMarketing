package smk.flow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import utils.DateUtils

/**
 *  用户流量活跃时段：
 *  // 1.  日活跃时段：分时段每日汇总
 *  2.  周活跃时段：分时段每周汇总
 *
 *  流量过滤规则：
 *    仅考虑 appid like 'C%'
 */
object FlowActivateHourW {

  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")
    // args0
    val start = param(0)
    // args1
    val end = if ( param.length < 2 ) DateUtils.dateAddAndFormat(start,6) else param(1) // '-' : 6 days later, or a specific date

    println(s"get args: $start, $end .")

    val conf = new SparkConf()
      .set("spark.executor.instances","8")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","8G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val t1 = System.currentTimeMillis()

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.flow_app_act_w (
        |  userid  string  comment "手机号",
        |  hour int comment "活跃时段",
        |  flow  int
        |)
        |COMMENT ' 目标用户APP流量活跃时段周表 '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val df = ssc.sql(
      s"""
         |select
         |  userid, hour, flow, part_type
         |from
         |(
         |  select
         |    userid, hour, sum(flow) as flow, part_type,
         |    row_number() over(partition by userid order by sum(flow) desc) as rank
         |  from suyanli.flow_app_stat_h
         |  where part_date between '$start' and '$end'
         |    and appid like 'C%'
         |  group by userid, hour, part_type
         |) t
         |where rank=1
         |""".stripMargin)

    df.withColumn("start_date",lit(start)).withColumn("end_date",lit(end))
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_type","start_date", "end_date")
      .insertInto("suyanli.flow_app_act_w")


    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
