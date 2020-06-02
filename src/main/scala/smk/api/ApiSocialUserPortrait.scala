package smk.api

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


object ApiSocialUserPortrait {


  def main(args: Array[String]): Unit = {

    var dur_con = "1=1"
    var cnt_con = "1=1"
    var pr_con = "1=1"
    var rank_con = "1=1"
    var type_con = "1=1"
    var month_con = "1=1"
    var limit_con = ""
    var filepath = "/suyan/chenzhihao/api_output/social_user_portrait_"+ new SimpleDateFormat("HHmmss").format(new Date)

    args.sliding(2,2).toList.collect{
      case Array("-cnt", c) =>    cnt_con += s" and score_cnt$c "
      case Array("-dur", c) =>  dur_con += s" and socre_dur$c "
      case Array("-pr", c) =>  pr_con += s" and score_pr$c "
      case Array("-rank", c) =>  rank_con += s" and rank_l$c "
      case Array("-type", e)   =>  type_con = s" type='$e' "
      case Array("-month", e)   =>  month_con = s" part_month='$e' "
      case Array("-limit", l) =>  limit_con = s" limit $l "
      case Array("-path", path) => filepath = path
    }

    println(      s"""
                     |select * from suyanli.social_user_portrait
                     |where $cnt_con and $dur_con and $pr_con and $rank_con and $type_con and $month_con
                     |$limit_con
                     |""".stripMargin)

    val conf = new SparkConf()
      .set("spark.executor.instances","2")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","4G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val res = ssc.sql(
      s"""
         |select * from suyanli.social_user_portrait
         |where $cnt_con and $dur_con and $pr_con and $rank_con and $type_con and $month_con
         |$limit_con
         |""".stripMargin)


    res.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(filepath)

    sc.stop()

  }

}
