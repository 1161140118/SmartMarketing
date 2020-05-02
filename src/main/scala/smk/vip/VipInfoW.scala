package smk.vip

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import utils.DateUtils

/**
  * 根据规则，提取 mall_user_trace_stat 中 user，去除商场员工等特殊人群，剩余重点客户为vip，
  * 按周统计该周内总访问时间，访问次数写入表 mall_vip_info_week
  */
object VipInfoW {

  def main(args: Array[String]): Unit = {

    // args0
    val start = args(0)
    // args1
    val end = if ( args(1)=="-" ) DateUtils.dateAddAndFormat(start,6) else args(1) // '-' : 6 days later, or a specific date
    // args2
    val mallIndex = 2 // index of param 'mall'
    val mall = if (args.length<= mallIndex || args(mallIndex).isEmpty ) "outlets" else args(mallIndex) // default : outlets

    val dateThreshold = if ( args(1)=="-" ) 4 else Math.max( DateUtils.dateDiff(start,end)/2 +1 , 1)
    println(s"get args: $start, $end, $mall. date thr: $dateThreshold.")

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc:SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    /**
      * mall_user_trace_stat: userid, dura, dura_sum, dura_avg, dura_avg_s, enter, leave, base_conn_cnt, base_uniq_cnt, level, part_date, part_mall
      *
      * 噪声数据清除规则：
      * 1. 根据记录时间剔除数据：
      *    1. 9-22 点以外
      *    2. 持续超过10小时剔除
      *    3. 平均连接时间为0min(<60s)
      *    4. 基站连接个数为1
      * 2. 根据日期上下文，一周内4天( 即最多不超过 间隔天数/2 ）以上出现者，为商场员工或附近居民
      * 3. 以当天持续时间长短作为衡量用户重要性依据之一
      * 4. 考虑到疫情影响 1.1-1.7, 1.8-1.14, 1.15-1.22 作为三个研究周期
      *
      */

    // -- mall_user_trace_stat: userid, dura, dura_sum, dura_avg, dura_avg_s, enter, leave, base_conn_cnt, base_uniq_cnt, level, part_date, part_mall

    val din = ssc.sql(
      s"""
         |select
         |  userid,
         |  count(*) as cnt,
         |  min(enter) as enter_min,
         |  max(leave) as leave_max,
         |  avg(dura) as timespan_avg,
         |  avg(dura_sum) as durasum_avg,
         |  sum(dura_sum) as durasum_sum,
         |  avg(base_conn_cnt) as base_conn_cnt_avg,
         |  avg(base_uniq_cnt) as base_uniq_cnt_avg,
         |  first(a.level) as level,
         |  '$start' as start_date,
         |  '$end' as end_date,
         |  '$mall' as part_mall
         |from
         |(
         |  select * from suyanli.mall_user_trace_stat
         |  where length(userid) = 11
         |    and enter between '09' and '22'
         |    and leave between '09' and '22'
         |    and dura between 20 and 600
         |    and dura_avg > 0
         |    and base_uniq_cnt > 1
         |    and part_mall = '$mall'
         |    and part_date between "$start" and "$end"
         |) a
         |group by userid
         |having count(*) <= '$dateThreshold'
       """.stripMargin)

    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    din.write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date", "part_mall")
      .insertInto("suyanli.mall_vip_info_week")

  }

}
