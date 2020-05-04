package smk.basicInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object BasicInfoOfTargetM {

  def main(args: Array[String]): Unit = {
    val month = args(0)
    println(s"get args: ${args(0)} .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances","4")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","16G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.basic_info_target_m (
        |  userid  string  comment "手机号",
        |  gender string,
        |  age int,
        |  birth_date  string,
        |  nation  string  comment "国籍",
        |  language  string  comment "语言"
        |)
        |COMMENT '个人用户基本信息月表 - 用户基本信息月表&个人客户基本信息日表'
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_month` string COMMENT '数据月份分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val b = ssc.sql(
      s"""
         |select
         |  userid, gender, age, birth_date, nation, language
         |from suyanli.basic_info_m
         |where part_month = '$month'
         |""".stripMargin)

    val t = ssc.sql(
      s"""
         |select userid, part_type
         |from
         |(
         |  select distinct userid, 'target' as part_type from suyanli.mall_target_portrait where start_date like '$month%'
         |  union
         |  select distinct userid, 'vip' as part_type from suyanli.mall_vip_portrait where start_date like '$month%'
         |) t
         |""".stripMargin)

    b.join(t, Seq("userid")).selectExpr("userid", "gender", "age", "birth_date", "nation", "language", "part_type", s"'$month' as part_month")
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_type", "part_month").insertInto("suyanli.basic_info_target_m")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
