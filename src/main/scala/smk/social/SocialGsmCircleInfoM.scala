package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object SocialGsmCircleInfoM {

  def main(args: Array[String]): Unit = {
    val month = args(0)

    println(s"get args: $month .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances", "4")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "16G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.social_gsm_stat_m (
        |  userid  string  comment "手机号",
        |  loc_cnt string  comment "不同地点记录数",
        |  calling_avg  int,
        |  called_avg   int,
        |  calling_cnt  int,
        |  called_cnt   int,
        |  calling_univ double  comment "通话特异性：平均时长/次数",
        |  one_min_calling_cnt int,
        |  local_call_cnt int comment "本地",
        |  toll_call_cnt  int comment "长途",
        |  roam_call_cnt  int comment "漫游"
        |)
        |COMMENT ' 用户语音信息统计月表 '
        |PARTITIONED BY (
        |    `part_month` string COMMENT '数据月份分区 <partition field>',
        |    `part_valid` string COMMENT '数据有效性/离群点: true / false <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val valid_userid = ssc.sql(
      s"""
         |select userid from suyanli.social_gsm_stat_m
         |where part_month = '$month' and part_valid = 'true'
         |""".stripMargin).registerTempTable("t_valid_userid")


    val circle = ssc.sql(
      s"""
         |select
         |  serv_no as userid,
         |  opp_serv_no as opp_userid,
         |  calling_dur,
         |  called_dur,
         |  calling_cnt,
         |  called_cnt,
         |  busy_call_cnt,
         |  idle_call_cnt
         |
         |
         |from
         |    (
         |      select * from suyanli.ti_ur_gsm_circle_m
         |      where part_month = '$month'
         |        and serv_no between '1' and '2'
         |        and opp_serv_no between '1' and '2'
         |        and opp_region_code = '451'
         |    ) a
         |    join
         |    ( select * from t_valid_userid  ) b on a.serv_no = b.userid
         |    join
         |    ( select * from t_valid_userid  ) c on a.serv_no = c.userid
         |
         |
         |""".stripMargin)

    circle.describe("call_cnt","calling_cnt","called_cnt","busy_call_cnt","idle_call_cnt").show(false)





    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")


  }
}