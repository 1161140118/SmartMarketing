package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object SocialMutualPortrait {

  def main(args: Array[String]): Unit = {
    val month = args(0)

    println(s"get args: $month .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances", "8")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "32G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.social_mutual_portrait (
        |  userid string  comment "拨打方号码",
        |  opp_userid string  comment "接听方号码",
        |  rank_avg  int  comment "通话平均时长排名",
        |  rank_cnt  int  comment "通话次数排名",
        |  rank_dis  int  comment "亲密度排名",
        |  score_dis double  comment "亲密度得分"
        |)
        |COMMENT ' 社交关系双方画像 '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_month` string COMMENT '数据月份分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val target = ssc.sql(
      s"""
         |select userid, part_type
         |from
         |(
         |  select distinct userid, 'target' as part_type from suyanli.mall_target_portrait where start_date like '$month%'
         |  union
         |  select distinct userid, 'vip' as part_type from suyanli.mall_vip_portrait where start_date like '$month%'
         |) t
         |""".stripMargin).cache()

    val dis =ssc.sql(
      s"""
         |select
         |  userid, opp_userid,
         |  calling_avg_r as rank_avg,
         |  calling_cnt_r as rank_cnt,
         |  score_r as rank_dis,
         |  call_score as score_dis
         |from suyanli.social_distance_m
         |where part_month = '$month'
         |""".stripMargin).join(target, Seq("userid"))

    dis.withColumn("part_month", lit(month)).write.mode("overwrite").format("parquet")
      .partitionBy("part_type","part_month").insertInto("suyanli.social_mutual_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }


}
