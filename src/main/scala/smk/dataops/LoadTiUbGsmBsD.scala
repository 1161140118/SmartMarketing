package smk.dataops

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object LoadTiUbGsmBsD {

  case class Member(
                     stat_date:String,
                     call_period:String,
                     serv_no:String,
                     lac:String,
                     ci:String,
                     bs_id:String,
                     total_call_dur:Long,
                     calling_dur:Long,
                     called_dur:Long,
                     call_cnt:Long,
                     calling_cnt:Long,
                     called_cnt:Long,
                     one_min_calling_cnt:Long,
                     local_call_cnt:Long,
                     toll_call_cnt:Long,
                     roam_call_cnt:Long
                   )

  def main(args: Array[String]): Unit = {
    val date = if( args(0)=="all") "*" else args(0)
    val t1 = System.currentTimeMillis()
    println(s"get args: $date.")

    val conf = new SparkConf()
      .set("spark.executor.instances","2")
      .set("spark.executor.memory","4G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sc.getConf.getAll.filter( _._1.contains("executor")).foreach(println) // print executor conf


    sc.textFile(s"hdfs:///suyan/ti_ub_gsm_bs_d_$date.txt")
      .map( _.split("\\|",16) )
      .map( x => Member( x(0), x(1), x(2), x(3), x(4), x(5), x(6).toLong, x(7).toLong, x(8).toLong, x(9).toLong, x(10).toLong, x(11).toLong, x(13).toLong, x(14).toLong, x(12).toLong, x(15).toLong ))
      .toDF.withColumn("part_date",$"stat_date")
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_date")
      .insertInto("suyanli.ti_ub_gsm_bs_d")

    sc.stop()

    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
