package smk.api

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  消费偏好画像API
 *  -shop 网购
 *  -pay  支付
 *  -fin  金融
 *  -hour 活跃时段
 *  -type
 *  -date 日期
 *  -limit
 *  -path
 *
 */
object ApiConsumeInterestPortrait {


  def main(args: Array[String]): Unit = {

    var shop_con = "1=1"
    var fin_con = "1=1"
    var pay_con  = "1=1"
    var hour_con = "1=1"
    var type_con = "1=1"
    var date_con = "1=1"
    var limit_con = ""
    var filepath = "/suyan/chenzhihao/api_output/consume_interest_portrait_"+ new SimpleDateFormat("HHmmss").format(new Date)

    args.sliding(2,2).toList.collect{
      case Array("-shop", c) =>  shop_con += s" and shop_score$c "
      case Array("-fin", c) =>  fin_con += s" and fin_score$c "
      case Array("-pay", c) =>  pay_con += s" and pay_score$c "
      case Array("-hour", c) =>  hour_con += s" and act_hour$c "
      case Array("-type", e)   =>  type_con = s" type='$e' "
      case Array("-date", e)   =>  date_con = s" part_date='$e' "
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
         |where $shop_con and $fin_con and $pay_con and $hour_con and $type_con and $date_con
         |$limit_con
         |""".stripMargin)


    res.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(filepath)

    sc.stop()

  }

}
