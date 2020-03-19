package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 2. 计算用户访问浏览信息
  *    5. 对每个用户聚集，标定当天浏览商场列表，count，平均持续时间， 加和持续时间
  */
object TargetMallStat {

  def main(args: Array[String]): Unit = {
    val start = args(0)
    val end = args(1)
    val t1 = System.currentTimeMillis()
    println(s"get args: $start, $end.")


    val sc = SparkContext.getOrCreate(new SparkConf().setAppName(getClass.getName.init))
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")




  }

}
