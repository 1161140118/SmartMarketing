package smk.flow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object FlowAppAnalysis {


  def main(args: Array[String]): Unit = {
    val date = args(0)
    println(s"get args: $date .")

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



    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
