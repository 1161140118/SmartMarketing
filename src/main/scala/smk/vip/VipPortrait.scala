package smk.vip

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import utils.DateUtils

/**
 * 核心客户画像（周）：
 * 1. 周访问次数
 * 2. 累计到该周，当月访问次数
 * 3. 周访问总/平均时长
 * 4. 累计到该周，当月访问总/平均时长
 * 5. 客户忠诚度打分：
 *    次数： 2  3  3.5
 *    时间： 0-1-2-3-4
 * 6. 客户忠诚度过滤：
 *    本周结算累计忠诚度低于0.3被过滤：
 *    1. 连续三周未出现的低活跃新顾客
 *    2. 连续五周未出现的高活跃新顾客
 *    3. 连续六周以上未出现的老顾客
 */
object VipPortrait {

  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")

    // args0
    val start = param(0)
    // args1
    val end = if ( param.length < 2 ) DateUtils.dateAddAndFormat(start,6) else param(1) // '-' : 6 days later, or a specific date

    val month = start.substring(0,6)
    val pre_start = if(param.length < 3 ) DateUtils.dateAddAndFormat(start, -7) else param(2)
    val pre_end = if(param.length < 4 ) DateUtils.dateAddAndFormat(pre_start, 6) else param(3)

    println(s"get args: $start, $end . more: $month, $pre_start, $pre_end ")
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
        |CREATE TABLE IF NOT  EXISTS suyanli.mall_vip_portrait (
        |  userid string,
        |  cnt_w int,
        |  cnt_m int,
        |  duration_w int,
        |  duration_m int,
        |  duration_avg_w int,
        |  duration_avg_m int,
        |  loyalty_cur double comment "本周忠诚度得分",
        |  loyalty double comment "累计忠诚度得分"
        |)
        |COMMENT ' 核心客户画像 '
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
         |  cnt,
         |  timespan_avg * cnt as timespan,
         |  timespan_avg,
         |  ln(timespan_avg+1) as timespan_avg_ln
         |from suyanli.mall_vip_info_week
         |where start_date = '$start'
         |  and end_date = '$end'
         |""".stripMargin)

    w.registerTempTable("t_week")

    val w_des = w.describe("timespan_avg_ln")
    val mean = w_des.where("summary = 'mean' ").collect()(0).getString(1).toDouble
    val stddev = w_des.where("summary = 'stddev' ").collect()(0).getString(1).toDouble

    // 依据： 一次逛很久 和 三次逛不多 等价
    // 次数： 2  3  3.5
    // 时间： 0-1-2-3-4
    // 该周忠诚度评分计算
    val w_score_cur = ssc.sql(
      s"""
         |select
         |  *,
         |  case cnt when 1 then 2
         |    when 2 then 3 else 3.5 end
         |    as cnt_score,
         |  case when timespan_avg_ln < $mean - 2*$stddev then 0
         |    when timespan_avg_ln > $mean + 2*$stddev then 4
         |    else (timespan_avg_ln - $mean)/$stddev +2 end
         |    as timespan_score
         |from t_week
         |""".stripMargin).withColumn("loyalty_cur", $"cnt_score"+$"timespan_score")

    w_score_cur.registerTempTable("t_week_cur")

    // 计算累计忠诚度 和 周访问记录
    val w_score = ssc.sql(
      s"""
         |select
         |  nvl(a.userid, b.userid) as userid,
         |  nvl(cnt, 0) as cnt_w,
         |  nvl(timespan, 0) as duration_w,
         |  nvl(timespan_avg, 0) as duration_avg_w,
         |  nvl(loyalty_cur, 0) as loyalty_cur,
         |  nvl(loyalty_cur,0 ) + nvl(b.loyalty, 0)/2 as loyalty
         |from
         |  ( select * from t_week_cur ) a
         |  full outer join
         |  ( select userid, loyalty from suyanli.mall_vip_portrait
         |    where start_date = '$pre_start' and end_date = '$pre_end' ) b -- 前一周
         |  on a.userid = b.userid
         |""".stripMargin).filter($"loyalty" > 0.3 ) // 筛除连续四周未出现的用户

    // 计算月访问记录
    val m = ssc.sql(
      s"""
         |select
         |  userid,
         |  cnt as cnt_m,
         |  timespan as duration_m,
         |  timespan / cnt as duration_avg_m
         |from
         |(
         |  select
         |    userid,
         |    sum(cnt) as cnt,
         |    sum(timespan_avg * cnt) as timespan
         |  from suyanli.mall_vip_info_week
         |  where start_date like '$month%'
         |    and end_date <= '$end'
         |  group by userid
         |) t
         |""".stripMargin)

    val df = w_score.as("a").join(m.as("b"), w_score("userid") === m("userid"), "full")
        .selectExpr(
          "nvl(a.userid, b.userid) as userid",
          "nvl(a.cnt_w, 0) as cnt_w",
          "nvl(b.cnt_m, 0) as cnt_m",
          "nvl(a.duration_w, 0) as duration_w",
          "nvl(b.duration_m, 0) as duration_m",
          "nvl(a.duration_avg_w, 0) as duration_avg_w",
          "nvl(b.duration_avg_m, 0) as duration_avg_m",
          "a.loyalty_cur as loyalty_cur",
          "a.loyalty as loyalty",
          s"'$start' as start_date",
          s"'$end' as end_date"
        )

    df.write.mode("overwrite").format("parquet")
      .partitionBy("start_date", "end_date")
      .insertInto("suyanli.mall_vip_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}
