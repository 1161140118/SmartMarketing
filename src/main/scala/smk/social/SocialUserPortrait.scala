package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object SocialUserPortrait {

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
        |CREATE TABLE IF NOT  EXISTS suyanli.social_user_portrait (
        |  userid string  comment "手机号码",
        |  score_dur  double  comment "通话时长评分",
        |  score_cnt  double  comment "通话次数评分",
        |  score_pr   double  comment "PageRank评分",
        |  rank_l int comment "pr局部排名",
        |  rank_g int comment "pr全局排名"
        |)
        |COMMENT ' 社交关系单方画像 '
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
         |""".stripMargin)

    val rank =ssc.sql(
      s"""
         |select userid, score, rank as rank_g
         |from suyanli.social_pagerank_m
         |where part_month = '$month'
         |""".stripMargin).join(target, Seq("userid"))

    val stat = ssc.sql(
      s"""
         |select
         |  userid,
         |  ln( nvl(calling_avg,0) * calling_cnt + nvl(called_avg,0) * called_cnt + 1) as call_dur,
         |  ln( calling_cnt + called_cnt + 1) as call_cnt
         |from suyanli.social_gsm_stat_m
         |where part_month='$month'
         |""".stripMargin).join(target, Seq("userid"))

    val des = stat.describe("call_dur","call_cnt")
    val mean = des.where("summary = 'mean' ").collect()(0).toSeq.tail.map( _.toString.toDouble )
    val stddev = des.where("summary = 'stddev' ").collect()(0).toSeq.tail.map( _.toString.toDouble )

    stat.withColumn("score_dur", ($"call_dur" - mean(0))/stddev(0)  )
        .withColumn("score_cnt", ($"call_cnt" - mean(1))/stddev(1))
        .join(rank, Seq("userid","part_type")).registerTempTable("t_score_rank")

    val res = ssc.sql(
      s"""
         |select
         |  userid,
         |  score_dur,
         |  score_cnt,
         |  score as score_pr,
         |  dense_rank() over ( order by rank_g ) as rank_l,
         |  rank_g,
         |  part_type,
         |  '$month' as part_month
         |from t_score_rank
         |""".stripMargin)

    res.write.mode("overwrite").format("parquet")
      .partitionBy("part_type","part_month").insertInto("suyanli.social_user_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}
