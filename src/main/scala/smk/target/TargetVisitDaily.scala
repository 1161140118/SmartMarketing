package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 2. 计算用户访问浏览信息
  *    5. 对每个用户聚集，标定当天浏览商场列表，count，平均持续时间， 加和持续时间
  */
object TargetVisitDaily {

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

    val df = ssc.sql(
      s"""
         |select
         |  userid,
         |  min(enter) as start_time,
         |  max(leave) as end_time,
         |  ( unix_timestamp(max(leave), 'HH:mm:ss') - unix_timestamp(min(enter), 'HH:mm:ss') )/60
         |   as timespan_total,
         |  avg(timespan) as timespan_avg,
         |  sum(dura_sum_min) as dura_sum_sum,
         |  avg(dura_sum_min) as dura_sum_avg,
         |  count(mall) as mall_cnt,
         |  collect_list(mall) as mall_list,
         |  sum( case when area='中心' then 1 else 0 end ) as area_center_cnt,
         |  sum( case when area='松北' then 1 else 0 end ) as area_songbei_cnt,
         |  sum( case when area='师大' then 1 else 0 end ) as area_shida_cnt,
         |  first(level) as level,
         |  part_date
         |from suyanli.mall_target_trace_stat
         |where part_date between '$start' and '$end'
         |group by userid, part_date
       """.stripMargin)

    df.write.mode("overwrite").format("parquet").partitionBy("part_date").insertInto("suyanli.mall_target_visit_daily")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
