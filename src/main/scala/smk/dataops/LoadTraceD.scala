package smk.dataops

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object LoadTraceD {

  case class Record(
                     day: String,
                     iccid1: String,
                     iccid2: String,
                     userid: String,
                     baseid: String,
                     basein: String,
                     startt: Long,
                     endt: Long,
                     level: Int
                   )

  def main(args: Array[String]): Unit = {
    val date = if (args(0) == "all") "" else args(0)
    val t1 = System.currentTimeMillis()
    println(s"get args: $date.")

    val conf = new SparkConf()
      .set("spark.executor.instances", "8")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "32G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sc.getConf.getAll.filter(_._1.contains("executor")).foreach(println) // print executor conf


    sc.textFile(s"hdfs:///suyan/tb_signal_mme_d_$date*")
      .map(_.split("\\|"))
      .map(x => Record(x(0), x(1), x(2), x(3), x(4), x(5), x(6).toLong, x(7).toLong, x(8).toInt))
      .toDF
      .selectExpr("iccid1", "iccid2", "userid", "baseid", "basein", "startt", "endt", "level", "day as part_date")
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_date")
      .insertInto("suyanli.s1mm_trace")

    sc.stop()

    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")


  }
}
