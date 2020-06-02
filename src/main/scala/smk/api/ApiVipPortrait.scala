package smk.api

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  核心客户画像查询 API
 *  1.  -cnt  指定周访问频率或其范围
 *  2.  -avg  指定周平均时长或其范围
 *  2.  -loyalty_cur  指定本周忠诚度或其范围
 *  3.  -loyalty  指定累计忠诚度或其范围
 *  4.  -start  指定开始日期
 *  5.  -end    指定结束日期
 *  6.  -limit  限制结果数量
 *  6.  -path   指定路径
 */

object ApiVipPortrait {

  def main(args: Array[String]): Unit = {

    var cnt_con = "1=1"
    var avg_con = "1=1"
    var loyalty_cur_con = "1=1"
    var loyalty_con = "1=1"
    var start_con = "1=1"
    var end_con = "1=1"
    var limit_con = ""
    var filepath = "/suyan/chenzhihao/api_output/vip_portrait_"+ new SimpleDateFormat("HHmmss").format(new Date)

    println(args.mkString(" ," ))
    args.sliding(2,2).toList.collect{
      case Array("-cnt", c) =>    cnt_con += s" and cnt_w$c "
      case Array("-avg", v) =>  avg_con += s" and duration_avg_w$v "
      case Array("-loyalty_cur", c) =>  loyalty_cur_con += s" and loyalty_cur$c "
      case Array("-loyalty", c) =>  loyalty_con += s" and loyalty$c "
      case Array("-start", s) =>  start_con = s" start_date<='$s' "
      case Array("-end", e)   =>  end_con = s" end_date>='$e' "
      case Array("-limit", l) =>  limit_con = s" limit $l "
      case Array("-path", path) => filepath = path
    }

    println(      s"""
                     |select * from suyanli.mall_vip_portrait
                     |where $cnt_con and $avg_con and $loyalty_con and $loyalty_cur_con and $start_con and $end_con
                     |$limit_con
                     |""")

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
         |select * from suyanli.mall_vip_portrait
         |where $cnt_con and $avg_con and $loyalty_con and $loyalty_cur_con and $start_con and $end_con
         |$limit_con
         |""".stripMargin)


    res.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(filepath)
    sc.stop()


  }

}
