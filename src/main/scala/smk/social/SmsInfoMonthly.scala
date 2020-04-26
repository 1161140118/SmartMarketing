package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * 点对点短信结算月表:
 *
 */
object SmsInfoMonthly {

  def main(args: Array[String]): Unit = {
    val month = args(0).split(',').mkString("\"",",","\"")

    println(s"get args: ${args(0)}, $month .")
    val t1 = System.currentTimeMillis()

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName(getClass.getName.init))
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    hc.sql(
      s"""
         |create table if not exists suyanli.social_sms_info_m(
         |  send_id string,
         |  reci_id string,
         |  sms_count int,
         |  user_type string
         |)
         |COMMENT '点对点短信记录数月表'
         |PARTITIONED BY (
         |    `part_month` string COMMENT '数据月份分区 <partition field>'
         |)
         |stored as parquet;
       """.stripMargin)

    // TODO sms_count 阈值
    val df = ssc.sql(
      s"""
         |select
         |  send_serv_no as send_id,
         |  reci_serv_no as reci_id,
         |  sms_count,
         |  user_type,
         |  part_month
         |from suyanli.ti_ub_p2p_sms_settle_m
         |where part_month in ($month)
         |  and home_city_code = '451'
         |  and opp_city_code = '451'
         |  and sms_count <  -- TODO
       """.stripMargin)

    df.write.mode("overwrite").format("parquet").partitionBy("part_month").insertInto("suyanli.social_sms_info_m")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
