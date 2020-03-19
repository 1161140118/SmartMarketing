package smk.vip

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CoreUserExtractor {

  def main(args: Array[String]): Unit = {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc:SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    /**
      * outlets 基站信息已存入 hive：
      * .toDF("mall","baseid","lng","lat")
      * .write.mode(SaveMode.Append).saveAsTable("suyanli.mall_cell_info")
      */


    //    val param = "aaaa"
//    val words = sc.parallelize(Seq("test","Successfully","Hello","Spark",param))
//    val wordCount = words.flatMap( _.split("") ).map( x => (x,1) ).reduceByKey( _+_ )
//    wordCount.map( l => (l._1,l._2)).toDF("letter","cnt").write.mode(SaveMode.Append).saveAsTable("suyanli.hellospark")

    sc.stop()

  }

}
