package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import utils.DateUtils


/**
  * 2. 计算用户访问浏览信息
  *    5. 对每个用户聚集，标定当天浏览商场列表，count，平均持续时间， 加和持续时间
  */
object TargetVisitAnalysis {

  object Util extends Serializable{

  }

  def main(args: Array[String]): Unit = {
    // args0
    val start = args(0)
    // args1
    val end = if ( args(1)=="-" ) DateUtils.dateAddAndFormat(start,6) else args(1) // '-' : 6 days later, or a specific date
    val dateThreshold = if ( args(1)=="-" ) 4 else Math.max( DateUtils.dateDiff(start,end)/2 +1 , 1)
    println(s"get args: $start, $end. date thr: $dateThreshold.")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances","2")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","8G")
    val sc = SparkContext.getOrCreate( conf )
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    /**
     *  blacklist
     */

    val hc = new HiveContext(sc)
    hc.sql(
      """
         |create table if not exists suyanli.mall_target_blacklist(
         |  userid  string,
         |  mall    string,
         |  visit_cnt int,
         |  min_date string
         |)
         |COMMENT '目标用户黑名单：用户-商场 疑似员工/住户表'
         |PARTITIONED BY (
         |    `start_date` string COMMENT '数据开始日期 <partition field>',
         |    `end_date`  string COMMENT '数据结束日期 <partition field>'
         |)
         |stored as parquet
       """.stripMargin)

    val blacklist = ssc.sql(
      s"""
         |select
         |  userid, mall, count(*) as cnt, min(part_date) as min_date,
         |  '$start' as start_date, '$end' as end_date
         |from suyanli.mall_target_trace_stat
         |where length(userid) = 11
         |  and part_date between "$start" and "$end"
         |group by userid, mall
         |having count(*) > $dateThreshold
       """.stripMargin)

    blacklist.write.mode("overwrite").format("parquet")
      .partitionBy("start_date", "end_date")
      .insertInto("suyanli.mall_target_blacklist")


    /**
     *  mall_target_visit_daily
     */

    val dd = ssc.sql(
      s"""
         |select
         |  a.userid as userid,
         |  min(enter) as start_time,
         |  max(leave) as end_time,
         |  ( unix_timestamp(max(leave), 'HH:mm:ss') - unix_timestamp(min(enter), 'HH:mm:ss') )/60
         |   as timespan_total,
         |  avg(timespan) as timespan_avg,
         |  sum(dura_sum_min) as dura_sum_sum,
         |  avg(dura_sum_min) as dura_sum_avg,
         |  count(a.mall) as mall_cnt,
         |  collect_list(a.mall) as mall_list,
         |  sum( case when area='中心' then 1 else 0 end ) as area_center_cnt,
         |  sum( case when area='松北' then 1 else 0 end ) as area_songbei_cnt,
         |  sum( case when area='师大' then 1 else 0 end ) as area_shida_cnt,
         |  first(level) as level,
         |  part_date
         |from
         |  ( select * from suyanli.mall_target_trace_stat where part_date between '$start' and '$end' ) a
         |  left outer join
         |  ( select userid, mall from suyanli.mall_target_blacklist ) b
         |  on a.userid == b.userid and a.mall = b.mall
         |where b.userid is null and length(a.userid) = 11
         |group by a.userid, part_date
       """.stripMargin)

    dd.write.mode("overwrite").format("parquet").partitionBy("part_date").insertInto("suyanli.mall_target_visit_daily")


    /**
     *  mall_target_visit_weekly
     */


    val mall_counter = (lists: Seq[String])  => {
      import scala.collection.mutable
      val list = lists.flatMap( _.split(',').map(_.drop(1).dropRight(1)) )
      val map: mutable.Map[String,Int] = mutable.Map()
      for(mall <- list){
        map += ( mall -> ( map.getOrElse(mall, 0 )+1) )
      }
      val arr = map.toArray[(String,Int)]
      arr.sortBy( _._2 ).reverse.toMap
    }
    val mall_counter_udf = udf(mall_counter)

    dd.registerTempTable("t_mall_target_visit_daily")

    val dw = ssc.sql(
      s"""
         |select
         |  userid,
         |  count(*) as visit_cnt,
         |  min(start_time) as start_min,
         |  max(end_time) as end_max,
         |  avg(timespan_total) as timespan_day_avg,
         |  avg(timespan_avg) as timespan_mall_avg,
         |  avg(dura_sum_sum) as dura_day_avg,
         |  sum(dura_sum_sum) / sum(mall_cnt) as dura_mall_avg,
         |  sum(mall_cnt) as mall_cnt,
         |  collect_list( mall_list ) as mall_list_tmp,
         |  sum(area_center_cnt) as area_center_cnt,
         |  sum(area_songbei_cnt) as area_songbei_cnt,
         |  sum(area_shida_cnt) as area_shida_cnt,
         |  '$start' as start_date,
         |  '$end' as end_date
         |from
         |  t_mall_target_visit_daily
         |group by userid
       """.stripMargin)
      .withColumn("mall_list", mall_counter_udf('mall_list_tmp))


    dw.select("userid", "visit_cnt", "start_min", "end_max",
      "timespan_day_avg", "timespan_mall_avg", "dura_day_avg", "dura_mall_avg",
      "mall_cnt", "mall_list", "area_center_cnt", "area_songbei_cnt", "area_shida_cnt",
      "start_date", "end_date")
      .write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date")
      .insertInto("suyanli.mall_target_visit_weekly")


    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
