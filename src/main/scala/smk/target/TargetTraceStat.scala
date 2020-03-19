package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 2. 计算用户访问浏览信息
  *    4. 对每个 用户-商场 聚集，计算持续时间，开始、结束时间
  */
object TargetTraceStat {

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

    val t = ssc.sql(
      s"""
         |select *
         |from
         |(
         |  select
         |    userid, mall,
         |    ( max(endt) - min(startt) )/60000 as timespan,
         |    sum(dura_sec)/60 as dura_sum_min,
         |    avg(dura_sec)/60 as dura_avg_min,
         |    avg(dura_sec)    as dura_avg_sec,
         |    from_unixtime( min( startt )/1000,'HH:mm:ss') as enter,
         |    from_unixtime( max( endt )/1000,'HH:mm:ss') as leave,
         |    area,
         |    first(level) as level,
         |    part_date
         |  from suyanli.mall_target_trace_info
         |  where part_date between '$start' and '$end'
         |  group by userid, mall, area, part_date
         |) t
         |where timespan between 20 and 360
         |  and dura_sum_min > 10
       """.stripMargin)
      .write.mode("overwrite").format("parquet").partitionBy("part_date")
      .insertInto("suyanli.mall_target_trace_stat")


    sc.stop()

    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
