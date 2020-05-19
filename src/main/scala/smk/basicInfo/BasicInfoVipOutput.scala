package smk.basicInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import utils.DateUtils

object BasicInfoVipOutput {


  def main(args: Array[String]): Unit = {
  val month = args(0)

    println(s"get args:  $month ")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances", "2")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "4G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val info = ssc.sql(
      s"""
         |select
         |  a.userid as userid, gender, age, birth_date, nation, language, lng, lat, days, part_type
         |from
         |  ( select * from suyanli.basic_info_target_m where part_month='$month' ) a
         |  left join
         |  ( select * from suyanli.basic_loc ) b
         |  on a.userid = b.userid
         |""".stripMargin)

    info.where(" days>2 ").count()

    val res = info.where(" part_type='vip' and days is not null")
      .unionAll(info.where(" part_type='target' and days is not null").sample(false, 0.2))

    res.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(s"/suyan/chenzhihao/bti")
    // hdfs dfs -get /suyan/chenzhihao/bti/part-00000 ~/chenzhihao/bti.csv

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}
