package smk.dataops

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object LoadTiIbIndivBaseD {
  case class Indiv(
                    stat_date:String,
                    cust_id:String,
                    cust_name:String,
                    city_code:String,
                    county_code:String,
                    cust_addr:String,
                    unit_name:String,
                    birth_month:String,
                    nation:String,
                    language:String,
                    nationality:String,
                    gender:String,
                    age:Int
                  )


  def main(args: Array[String]): Unit = {
    val date = if( args(0)=="all") "*" else args(0)
    val t1 = System.currentTimeMillis()
    println(s"get args: $date.")

    val conf = new SparkConf()
      .set("spark.executor.instances","4")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","16G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sc.getConf.getAll.filter( _._1.contains("executor")).foreach(println) // print executor conf

    sc.textFile(s"hdfs:///suyan/ti_ib_indiv_base_d_$date.txt")
      .map( r => {
        val x = r.split("\\|", 20)
        if (x.length == 13) {
          Indiv(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12).toInt)
        } else {
          val l = x.length
          Indiv(x(0), x(1), x(2), x(3), x(4), x.slice(5, l - 7).mkString("/"), x(l - 7), x(l - 6), x(l - 5), x(l - 4), x(l - 3), x(l - 2), x(l - 1).toInt)
        }
      })
      .toDF().withColumn("part_date",$"stat_date")
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_date").insertInto("suyanli.ti_ib_indiv_base_d")

    sc.stop()

    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
