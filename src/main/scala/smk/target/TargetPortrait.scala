package smk.target

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.DateUtils

/**
 * 目标客户画像（周）：
 * 1. 周访问次数
 * 2. 累计到该周，当月访问次数
 * 3. 周访问总/平均时长
 * 4. 累计到该周，当月访问总/平均时长
 */
object TargetPortrait {

  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")

    // args0
    val start = param(0)
    // args1
    val end = if ( param.length < 2 ) DateUtils.dateAddAndFormat(start,6) else param(1) // '-' : 6 days later, or a specific date

    val month = start.substring(0,6)
//    val pre_start = if(param.length < 3 ) DateUtils.dateAddAndFormat(start, -7) else param(2)
//    val pre_end = if(param.length < 4 ) DateUtils.dateAddAndFormat(pre_start, 6) else param(3)

    println(s"get args: $start, $end . more: $month ")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances", "4")
      .set("spark.executor.cores", "2")
      .set("spark.executor.memory", "8G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.mall_target_portrait (
        |  userid string,
        |  cnt_w int  comment "商场访问次数",
        |  cnt_m int,
        |  duration_w int comment "商场访问总时长",
        |  duration_m int,
        |  duration_avg_w int comment "商场平均访问时长",
        |  duration_avg_m int,
        |  act_weekday  double  comment "工作日平均活跃度",
        |  act_weekend  double  comment "周末平均活跃度",
        |  act_w  double,
        |  act_m  double
        |)
        |COMMENT ' 目标客户画像 '
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    // 该周记录
    val w = ssc.sql(
      s"""
         |select
         |  userid,
         |  mall_cnt as cnt_w,
         |  timespan_mall_avg * mall_cnt as duration_w,
         |  timespan_mall_avg as duration_avg_w
         |from suyanli.mall_target_visit_weekly
         |where start_date = '$start'
         |  and end_date = '$end'
         |""".stripMargin)

    val m = ssc.sql(
      s"""
         |select
         |  userid,
         |  mall_cnt as cnt_m,
         |  timespan_mall as duration_m,
         |  timespan_mall / mall_cnt as duration_avg_m
         |from
         |(
         |  select
         |    userid,
         |    sum(mall_cnt) as mall_cnt,
         |    sum(timespan_mall_avg * mall_cnt) as timespan_mall
         |  from suyanli.mall_target_visit_weekly
         |  where start_date like '$month%'
         |    and end_date <= '$end'
         |  group by userid
         |) t
         |""".stripMargin)

    val dura = w.as("a").join(m.as("b"), w("userid") === m("userid"), "full")
      .selectExpr(
        "nvl(a.userid, b.userid) as userid",
        "nvl(a.cnt_w, 0) as cnt_w",
        "nvl(b.cnt_m, 0) as cnt_m",
        "nvl(a.duration_w, 0) as duration_w",
        "nvl(b.duration_m, 0) as duration_m",
        "nvl(a.duration_avg_w, 0) as duration_avg_w",
        "nvl(b.duration_avg_m, 0) as duration_avg_m"
      )
    dura.registerTempTable("t_dura")

    val act = ssc.sql(
      s"""
         |select a.userid as userid, act_weekday, act_weekend, act_w, act_m
         |from
         |(
         |  select
         |    userid,
         |    sum(case part_dtype when 'weekday' then act_d else 0 end)/sum(case part_dtype when 'weekday' then 1 else 0 end) as act_weekday,
         |    sum(case part_dtype when 'weekend' then act_d else 0 end)/sum(case part_dtype when 'weekend' then 1 else 0 end) as act_weekend
         |  from  suyanli.mall_target_act_d
         |  where part_date between '$start' and '$end'
         |  group by userid
         |) a
         |join
         |( select userid, act_w, act_m from suyanli.mall_target_act_d where part_date='$end' ) b
         |on a.userid = b.userid
         |""".stripMargin)
    act.registerTempTable("t_act")


    val df = ssc.sql(
      s"""
         |select
         |  a.userid as userid, cnt_w, cnt_m, duration_w, duration_m, duration_avg_w, duration_avg_m,
         |  act_weekday, act_weekend, act_w, act_m,
         |  '$start' as start_date, '$end' as end_date
         |from
         |( select * from t_dura ) a
         |left join
         |( select * from t_act ) b
         |on a.userid = b.userid
         |""".stripMargin)

    df.write.mode("overwrite").format("parquet")
      .partitionBy("start_date", "end_date")
      .insertInto("suyanli.mall_target_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}
