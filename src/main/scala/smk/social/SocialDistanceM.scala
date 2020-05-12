package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object SocialDistanceM {

  def main(args: Array[String]): Unit = {
    val month = args(0)
    println(s"get args: $month .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances","8")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","16G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.social_distance_m (
        |  userid  string  comment "主叫方手机号",
        |  opp_userid string comment "被叫方手机号",
        |  score_r int comment "通话得分相对主叫方排名",
        |  call_score double  comment "通话得分",
        |  calling_avg_r int,
        |  calling_cnt_r int,
        |  calling_days_r int,
        |  called_avg_r int,
        |  called_cnt_r int,
        |  called_days_r int
        |)
        |COMMENT ' 社交 亲密度/距离 评分  '
        |PARTITIONED BY (
        |    `part_month` string COMMENT '数据月份分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val dis = ssc.sql(
      s"""
         |select
         |  * , row_number() over (partition by userid order by call_score desc) as score_r
         |from
         |(
         |  select
         |    * ,
         |    3/( 1/called_avg_r + 1/called_cnt_r + 1/called_days_r  ) as called_dis,
         |    2 / ( calling_dis +  3/(1/called_avg_r + 1/called_cnt_r + 1/called_days_r) ) as call_score
         |  from
         |  (
         |    select * ,
         |      rank() over (partition by opp_userid order by called_dur*( 1- busy_dur*60/(calling_dur+called_dur) )/called_cnt desc ) as called_avg_r,
         |      rank() over (partition by opp_userid order by called_cnt*( 1- busy_cnt/(calling_cnt+called_cnt) ) desc ) as called_cnt_r,
         |      rank() over (partition by opp_userid order by call_days desc, call_days_span asc) as called_days_r,
         |      3/( 1/calling_avg_r + 1/calling_cnt_r + 1/calling_days_r  ) as calling_dis
         |    from
         |      (
         |        select * ,
         |          rank() over (partition by userid order by calling_dur*( 1- busy_dur*60/(calling_dur+called_dur) )/calling_cnt desc ) as calling_avg_r,
         |          rank() over (partition by userid order by calling_cnt*( 1- busy_cnt/(calling_cnt+called_cnt) ) desc ) as calling_cnt_r,
         |          rank() over (partition by userid order by call_days desc, call_days_span asc) as calling_days_r
         |        from suyanli.social_gsm_circle_info_m
         |        where part_month='$month'
         |      ) t
         |  ) tt
         |) ttt
         |""".stripMargin)

    dis.select("userid", "opp_userid", "score_r", "call_score", "calling_avg_r", "calling_cnt_r", "calling_days_r", "called_avg_r", "called_cnt_r", "called_days_r")
      .withColumn("part_month", lit(month))
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_month").insertInto("suyanli.social_distance_m")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
