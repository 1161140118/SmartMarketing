package smk.basicInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext

object BasicLoc {


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

    /**
     * 获得位置坐标
     * 注意：本部分不应被重复执行
     */
    // 读忠旭的居民位置坐标
    val loc = sc.textFile("hdfs:///suyan/lzx/bishe/zhu_loc/part-r-00000")
      .map( r => r.split("\\|") ).map( r =>
      try {    ( r(0).trim, r(1).trim.toDouble, r(2).trim.toDouble, r(3).trim.toInt )   }
      catch {    case e: NumberFormatException => null   }
    ).filter( _ != null ).toDF("userid", "lng", "lat", "days")
    loc.saveAsTable("suyanli.basic_loc")

    loc.groupBy("days").count().show()
    loc.select("userid").distinct().count()

    /**
     * 过滤目标用户
     */

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.basic_loc_target (
        |  userid string  comment "拨打方号码",
        |  lng  double,
        |  lat  double,
        |  days int
        |)
        |COMMENT ' 目标客户常驻地坐标 '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_month` string COMMENT '数据月份分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val t = ssc.sql(
      s"""
         |select userid, part_type
         |from
         |(
         |  select distinct userid, 'target' as part_type from suyanli.mall_target_portrait where start_date like '$month%'
         |  union
         |  select distinct userid, 'vip' as part_type from suyanli.mall_vip_portrait where start_date like '$month%'
         |) t
         |""".stripMargin).cache()
    val loc_info = loc.join(t, Seq("userid"))

    loc_info.withColumn("part_month", lit(month)).write.mode("overwrite").format("parquet")
      .partitionBy("part_type","part_month").insertInto("suyanli.basic_loc_target")

    loc_info.groupBy("part_type").count()

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }



}
