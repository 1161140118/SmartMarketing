package smk.vip

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object UserTraceStat {

  def main(args: Array[String]): Unit = {

    val start = args(0)
    val end = args(1)
    val mall = if (args.length<=2 || args(2).isEmpty ) "outlets" else args(2) // default : outlets
    println(s"get args: $start, $end, $mall.")

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc:SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    val stat = ssc.sql(
      s"""
         |select
         |   userid, part_date, part_mall,
         |   ( max(endt) - min(startt) )/60000 as dura,
         |   sum( duration ) as dura_sum_s,
         |   avg( duration ) as dura_avg_s,
         |   from_unixtime( min( startt )/1000,'HH:mm:ss') as enter,
         |   from_unixtime( max( endt )/1000,'HH:mm:ss') as leave,
         |   count(*) as base_conn_cnt,
         |   count(distinct baseid) as base_uniq_cnt,
         |   first(level) as level
         |from
         |   (select
         |      *, (endt-startt)/1000 as duration
         |    from suyanli.mall_user_trace_info
         |    where part_date between '$start' and '$end'
         |      and part_mall='$mall' ) a
         |group by userid, part_date, part_mall
    """.stripMargin)


    // 写入hive
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    stat.selectExpr("userid", "dura", "dura_sum_s/60 as dura_sum", "dura_avg_s/60 as dura_avg", "dura_avg_s", "enter", "leave", "base_conn_cnt", "base_uniq_cnt", "level", "part_date", "part_mall")
      .write.mode("overwrite").format("parquet").partitionBy("part_date","part_mall")
      .insertInto("suyanli.mall_user_trace_stat")

  }

}
