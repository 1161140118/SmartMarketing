package smk.api

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 *  购物意图画像API
 *  -cnt  线下访问次数
 *  -visit_incr 线下访问增长率
 *  -act_incr 活跃度增长率
 *  -shop 线上购物强度
 *  -shop_incr  线上购物强度增长率
 *  -type
 *  -start
 *  -end
 *  -limit
 *  -path
 *
 */
object ApiShoppingIntentionPortrait {


  def main(args: Array[String]): Unit = {

    var cnt_con = "1=1"
    var visit_incr_con = "1=1"
    var act_incr_con  = "1=1"
    var shop_con = "1=1"
    var shop_incr_con = "1=1"
    var type_con = "1=1"
    var start_con = "1=1"
    var end_con = "1=1"
    var limit_con = ""
    var filepath = "/suyan/chenzhihao/api_output/shopping_intention_portrait_"+ new SimpleDateFormat("HHmmss").format(new Date)

    args.sliding(2,2).toList.collect{
      case Array("-cnt", c) =>  cnt_con += s" and visit_cnt$c "
      case Array("-visit_incr", c) =>  visit_incr_con += s" and visit_incr$c "
      case Array("-act_incr", c) =>  act_incr_con += s" and act_incr$c "
      case Array("-shop", c) =>  shop_con += s" and shop_socre$c "
      case Array("-shop_incr", c) =>  shop_incr_con += s" and shop_incr$c "
      case Array("-type", e)   =>  type_con = s" type='$e' "
      case Array("-start", e)   =>  start_con = s" start_date='$e' "
      case Array("-end", e)   =>  end_con = s" end_date='$e' "
      case Array("-limit", l) =>  limit_con = s" limit $l "
      case Array("-path", path) => filepath = path
    }



    val conf = new SparkConf()
      .set("spark.executor.instances","2")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","4G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val res = ssc.sql(
      s"""
         |select * from suyanli.social_mutual_portrait
         |where $cnt_con and $visit_incr_con and $act_incr_con and $shop_con and $shop_incr_con  and $type_con and $start_con and $end_con
         |$limit_con
         |""".stripMargin)


    res.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(filepath)

    sc.stop()

  }

}
