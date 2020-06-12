package smk.flow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
 *  目标客户流量数据，时段表/日表
 */
object FlowAppStatD {


  def main(args: Array[String]): Unit = {
    val date = args(0)
    val mode = if (args.length>=1 && Seq("h","d","all").contains(args(1)) )  args(1) else "all"
    println(s"get args: $date , $mode.")

    val conf = new SparkConf()
      .set("spark.executor.instances","8")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","16G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val t1 = System.currentTimeMillis()

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.flow_app_stat_h (
        |  userid  string  comment "手机号",
        |  appid  string,
        |  appname  string,
        |  hour   int,
        |  flow  int
        |)
        |COMMENT ' 目标用户APP流量数据分时段表 '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_date` string COMMENT '数据日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.flow_app_stat_d (
        |  userid  string  comment "手机号",
        |  appid  string,
        |  appname  string,
        |  flow  int
        |)
        |COMMENT ' 目标用户APP流量数据日表 '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_date` string COMMENT '数据日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val target = ssc.sql(
      s"""
         |select userid, part_type
         |from
         |(
         |  select distinct userid, 'target' as part_type
         |  from suyanli.mall_target_portrait
         |  where start_date <= $date and end_date >= $date
         |  union
         |  select distinct userid, 'vip' as part_type
         |  from suyanli.mall_vip_portrait
         |  where start_date <= $date and end_date >= $date
         |) t
         |""".stripMargin).cache()

    val flow = ssc.sql(
      s"""
         |select
         |  userid, flow, appid, appname, hour
         |from suyanli.ts_hit_app
         |where part_date='$date'
         |""".stripMargin).join(target, Seq("userid"))

    if (mode == "h" || mode=="all"){
      val hour = flow.groupBy('userid, 'appid, 'appname, 'part_type, 'hour).agg( sum("flow").as("flow") )
        .selectExpr("userid", "appid",  "appname", "hour", "flow", "part_type", s"'$date' as part_date")
      hour.write.mode("overwrite").format("parquet").partitionBy("part_type", "part_date").insertInto("suyanli.flow_app_stat_h")
    }

    if (mode == "d" || mode=="all") {
      val day = flow.groupBy('userid, 'appid, 'appname, 'part_type).agg(sum("flow").as("flow"))
        .selectExpr("userid", "appid", "appname", "flow", "part_type", s"'$date' as part_date")
      day.write.mode("overwrite").format("parquet").partitionBy("part_type", "part_date").insertInto("suyanli.flow_app_stat_d")
    }

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }
}
