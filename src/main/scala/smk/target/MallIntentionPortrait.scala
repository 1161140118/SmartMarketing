package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import utils.DateUtils

/**
 *  <h2>购物意图画像综合:</h2>
 *  <b>注意：依赖前一周.</b>
 *  1.  线上购物意图
 *    1.  购物类APP使用强度：从flow analysis拉出日数据，取周平均
 *    2.  近期线上购物倾向（周）：本周取平均与上周数据取平均对比
 *    3.  APP使用活跃时段（周）：从 flow_app_act_w 拉取
 *  2.  线下购物意图
 *    1.  商场访问频次：从画像表中拉出，综合目标、核心客户
 *    2.  商场访问强度：同上
 *    3.  近期线下消费倾向：总时长（次数*平均时长）变化量
 *    4.  近期线下活跃倾向：
 *        vip: 忠诚度变化量
 *        target: 活跃度变化量
 */
object MallIntentionPortrait {


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
        |CREATE TABLE IF NOT  EXISTS suyanli.mall_intention_portrait (
        |  userid string,
        |  visit_cnt  int   comment "线下商场访问次数",
        |  visit_avg  int  comment "线下商场访问平均时长/min",
        |  visit_incr double  comment "线下商场访问时长增长率",
        |  act_incr   double  comment "线下活跃度/忠诚度增长率",
        |  shop_score double  comment "线上购物APP使用得分",
        |  shop_incr  double  comment "线上购物APP使用得分增长量",
        |  act_hour int comment "周活跃时段"
        |)
        |COMMENT '购物意图画像(核心&目标)'
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    /**
     *  online
     */

    val online = ssc.sql(
      s"""
         |select
         |  a.userid as userid, a.part_type as part_type, a.shop_score as shop_score,
         |  if(b.shop_score=0, 0, nvl(a.shop_score - b.shop_score, 0)) as shop_incr,
         |  nvl(f.hour, -1) as act_hour
         |from
         |( select userid, part_type, avg(shop_score) as shop_score
         |  from suyanli.flow_app_portrait  where part_date between '$start' and '$end'
         |  group by userid, part_type) a
         |left join
         |( select userid, part_type, avg(shop_score) as shop_score
         |  from suyanli.flow_app_portrait  where part_date between '$pre_start' and '$pre_end'
         |  group by userid, part_type ) b
         |on a.userid=b.userid and a.part_type=b.part_type
         |left join
         |( select * from suyanli.flow_app_act_w where start_date>='$pre_start' and end_date<='$end') f
         |on a.userid=f.userid and a.part_type=f.part_type
         |""".stripMargin)

    online.where("part_type='vip' and act_hour>0").show()

    /**
     *  offline
     */

    val visit_info = ssc.sql(
      s"""
         |select userid, cnt_w as cnt, dur_avg_w as dur_avg, duration_w as duration, act_w as act, part_type, start_date, end_date
         |from
         |(
         |  select userid, cnt_w, duration_w as dur_avg_w, cnt_w * duration_w as duration_w, loyalty_cur as act_w, 'vip' as part_type, start_date, end_date
         |  from suyanli.mall_vip_portrait  where start_date>='$pre_start' and end_date<='$end'
         |  union
         |  select userid, cnt_w, duration_w as dur_avg_w, cnt_w * duration_w as duration_w, act_w, 'target' as part_type, start_date, end_date
         |  from suyanli.mall_target_portrait  where start_date>='$pre_start' and end_date<='$end'
         |) t
         |""".stripMargin)
    visit_info.registerTempTable("t_visit")

    val offline = ssc.sql(
      s"""
         |select
         |  a.userid as userid, a.cnt as visit_cnt, a.dur_avg as dur_avg,
         |  nvl(a.duration/b.duration -1, 0) as dur_incr,
         |  nvl(a.act/b.act -1, 0) as act_incr,
         |  a.part_type as part_type
         |from
         |( select * from t_visit where start_date>='$start' and end_date<='$end' ) a
         |left join
         |( select * from t_visit where start_date>='$pre_start' and end_date<='$pre_end' ) b
         |on a.userid=b.userid and a.part_type=b.part_type
         |""".stripMargin)

    online.registerTempTable("t_online")
    offline.registerTempTable("t_offline")

    val res = ssc.sql(
      s"""
         |select
         |  f.userid as userid, visit_cnt, dur_avg, dur_incr, act_incr,
         |  shop_score, shop_incr, act_hour,
         |  f.part_type as part_type,
         |  '$start' as start_date,
         |  '$end'  as end_date
         |from
         |( select * from t_offline ) f
         |left join
         |( select * from t_online ) n
         |on f.userid=n.userid and f.part_type=n.part_type
         |""".stripMargin)

    res.write.mode("overwrite").format("parquet")
      .partitionBy("part_type","start_date", "end_date")
      .insertInto("suyanli.mall_intention_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }


}
