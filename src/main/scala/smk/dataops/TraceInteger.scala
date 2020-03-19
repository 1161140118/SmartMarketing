package smk.dataops

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * 将多个 s1mm 文件整理导入到分区表 s1mm_tarce 中。
  */
object TraceInteger {

  def main(args: Array[String]): Unit = {

    val start = args(0).toInt
    val end = args(1).toInt

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    for( date <- start to end){
      ssc.sql(s"select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from suyanli.s1mm_$date")
        .write.mode(SaveMode.Overwrite).format("parquet").partitionBy("part_date").insertInto("suyanli.s1mm_trace")
    }


    ssc.sql(
      s"""
         |insert overwrite table suyanli.s1mm_trace partition(`part_date`)
         |select iccid1, iccid2, userid, baseid, basein, startt, endt, `level`, `day` as part_date from suyanli.s1mm_20200101;
       """.stripMargin)

    sc.stop()

  }

}
