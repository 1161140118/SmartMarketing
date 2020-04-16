package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

/**
  * 2. 计算用户访问浏览信息
  *    4. 对每个 用户-商场 聚集，计算持续时间，开始、结束时间
  */
object TargetTraceStat {

  case class Trace (
                   userid: String,
                   mall: String,
                   timespan: Int,
                   dura_sum_min: Int,
                   dura_avg_min: Int,
                   dura_avg_sec: Int,
                   enter: String,
                   leave: String,
                   area: String,
                   level:Int,
                   part_date:String
                   )

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
         |select *
         |from
         |(
         |  select
         |    part_date,
         |    userid, mall,
         |    ( max(endt) - min(startt) )/60000 as timespan,
         |    sum(dura_sec)/60 as dura_sum_min,
         |    avg(dura_sec)/60 as dura_avg_min,
         |    avg(dura_sec)    as dura_avg_sec,
         |    from_unixtime( min( startt )/1000,'HH:mm:ss') as enter,
         |    from_unixtime( max( endt )/1000,'HH:mm:ss') as leave,
         |    area,
         |    first(level) as level,
         |    min(startt)/1000 as enter_ttp,
         |    max(endt)/1000   as leave_ttp
         |  from
         |   (
         |      select distinct *
         |      from  suyanli.mall_target_trace_info
         |      where part_date between '$start' and '$end'
         |   ) a
         |  group by userid, mall, area, part_date
         |) t
         |where timespan between 20 and 360
         |  and dura_sum_min > 10
       """.stripMargin)

    // 去除20min内出现多个记录的情况
    val t =
      df.rdd.groupBy( r => ( r.getString(0), r.getString(1) ) ) // gourp by userid, part_date
      .mapValues( v => {
        val values = v.map( r => Tuple3[Row,Double,Double](r, r.getAs[Double]("enter_ttp"), r.getAs[Double]("leave_ttp") ) )
        .toList
        .sortBy( _._2 )

        val valid = new Array[Boolean](values.length) // mark valid of values
        for(i <- 0 to values.length-1) valid(i)=true
        // 开始时间相差20min以内，或前者结束时间晚于后者，过滤
        for(i <- 0 to values.length-2 if ( values(i+1)._2 - values(i)._2 < 1200 || values(i)._3 > values(i+1)._3 )) valid(i) = false

        val res = ListBuffer[Row]()
        for(i <- 0 to values.length-1 if valid(i) )  res += values(i)._1

        res
          .map( r =>
            Trace(
              r.getString(1), r.getString(2), r.getDouble(3).toInt,
              r.getDouble(4).toInt, r.getDouble(5).toInt, r.getDouble(6).toInt,
              r.getString(7), r.getString(8), r.getString(9), r.getInt(10), r.getString(0)
            )
          )
    } ).flatMap( r => r._2 ).toDF()

    t.write.mode("overwrite").format("parquet").partitionBy("part_date")
      .insertInto("suyanli.mall_target_trace_stat")

    sc.stop()

    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
