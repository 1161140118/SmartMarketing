package smk.flow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object FlowAppStatD {


  def main(args: Array[String]): Unit = {
    val date = args(0)
    println(s"get args: $date .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances","8")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","32G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
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
         |""".stripMargin)


    val flow = ssc.sql(
      s"""
         |select
         |  userid, flow, appid, appname
         |from suyanli.ts_hit_app
         |where part_date='$date'
         |""".stripMargin)

    val res = flow.join(target, Seq("userid")).groupBy('userid, 'appid, 'appname, 'part_type).agg( sum("flow").as("flow") )
      .selectExpr("userid",  "appid",  "appname", "flow", "part_type", s"'$date' as part_date")

    res.write.mode("overwrite").format("parquet").partitionBy("part_type", "part_date").insertInto("suyanli.flow_app_stat_d")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }
}
