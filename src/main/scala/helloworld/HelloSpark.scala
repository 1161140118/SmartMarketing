package helloworld

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object HelloSpark {

  def main(args: Array[String]): Unit = {

    for( a <- args){
      println(s"arg: $a")
    }

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc:SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    val param = "aaaa"
    val words = sc.parallelize(Seq("test","Successfully","Hello","Spark",param))
    val wordCount = words.flatMap( _.split("") ).map( x => (x,1) ).reduceByKey( _+_ )
    wordCount.map( l => (l._1,l._2)).toDF("letter","cnt").write.mode(SaveMode.Append).saveAsTable("suyanli.hellospark")

//    wordCount.repartition(1).saveAsTextFile("hdfs:///suyan/hellospark.txt")
//    ssc.createDataFrame(wordCount.map( l => (l._1,l._2))).write.mode(SaveMode.Append).saveAsTable("suyanli.hellospark")


    sc.stop()
  }

}
