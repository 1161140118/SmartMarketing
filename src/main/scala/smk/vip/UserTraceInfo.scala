package smk.vip

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * 匹配用户和基站，过滤得到轨迹记录
 */
object UserTraceInfo {

  def main(args: Array[String]): Unit = {

    val start = args(0)
    val end = args(1)
    val mall = if (args.length<=2 || args(2).isEmpty ) "outlets" else args(2) // default : outlets
    println(s"get args: $start, $end, $mall.")

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc:SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    val cell = ssc.sql(s"select distinct baseid from suyanli.mall_cell_info where mall='$mall' ") // "mall","baseid","lng","lat"
    val trace = ssc.sql(s"select userid, baseid, startt, endt, level, part_date from suyanli.s1mm_trace where part_date between '$start' and '$end' ")
    val outlets = trace.join(cell,Seq("baseid"))

    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    import org.apache.spark.sql.functions._
    outlets.selectExpr("userid", "baseid", "startt", "endt", "level", "part_date", s"'$mall' as part_mall")
      .write.mode("overwrite").format("parquet").partitionBy("part_date","part_mall")
      .insertInto("suyanli.mall_user_trace_info")

    sc.stop()

  }

}
