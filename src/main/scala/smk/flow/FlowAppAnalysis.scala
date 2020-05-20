package smk.flow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/**
 *  APP 使用情况分析
 *  1.  网购类APP：淘宝等
 *  2.  支付类APP
 *  3.  金融类APP
 *
 *  评估策略：
 *  1.  每个APP使用流量对数化，并根据偏离对数均值程度打分
 *  2.  同类APP使用取打分最高者作为该类APP使用情况得分
 *
 */

object FlowAppAnalysis {


  def main(args: Array[String]): Unit = {
    val date = args(0)
    println(s"get args: $date .")

    val conf = new SparkConf()
      .set("spark.executor.instances","4")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","8G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val t1 = System.currentTimeMillis()


    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.flow_app_analysis_d (
        |  userid  string  comment "手机号",
        |  shop_score double  comment "网购类APP使用得分:0-4",
        |  pay_score double  comment "支付类APP使用得分:0-4",
        |  fin_score double  comment "金融类APP使用得分:0-4"
        |)
        |COMMENT ' 目标用户APP流量分类使用情况日表 '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_date` string COMMENT '数据日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val gp_cols = List("userid","part_type","part_date")

    val shop_appids = List("C874", "C1655", "C65307", "C1006", "C632", "C923", "W142", "C66988")
    val pay_appids = List("C1167", "C65816", "C4518")
    val fin_appids = List("C66739", "C222343","C968")
    val apps = (shop_appids:::pay_appids:::fin_appids).mkString("(\'","\',\'","\')")

    val flow = ssc.sql(
      s"""
         |select *, ln(flow) as flow_ln
         |from suyanli.flow_app_stat_d
         |where appid in $apps and part_date='$date'  and flow >0
         |""".stripMargin)

//    val sapp_gb = shop_appids.foldLeft(sflow){
//      case (data, appid) => data.selectExpr("*",s"IF(appid='$appid', flow, 0) as $appid")
//    }.drop("appid").drop("appname").drop("flow")
//        .groupBy("userid","part_type","part_date")
////        .agg( sum("C874").as("C874"), sum("C1655").as("C1655")), sum("C65307").as("C65307")), sum("C1006").as("C1006")), sum("C632").as("C632")), sum("C923").as("C923")), sum("W142").as("W142")), sum("C66988").as("C66988")) )
//        .sum( shop_appids:_* )
//        .selectExpr( List("userid","part_type","part_date") ::: shop_appids.map(appid => s" `sum($appid)` as $appid " ):_*)

    def myVar(col: Column): Column = {  sqrt(avg(col * col) - avg(col) * avg(col)) }
    val stat = flow.groupBy("appid","appname").agg( avg("flow_ln").as("flow_avg"), myVar($"flow_ln").as("flow_var") ).cache()
    stat.registerTempTable("t_stat")

    val flow_score = flow.join(stat, Seq("appid","appname")).selectExpr(
      "userid","appid","appname",
          "case when flow_ln>flow_avg+2*flow_var then 0 when flow_ln<flow_avg-2*flow_var then 4 else (flow_ln-flow_avg)/flow_var +2 end as flow_score ",
          "flow_ln", "part_type", "part_date"
        )

    // 网购类APP
    val shop_score = flow_score.where($"appid".isin(shop_appids:_*)).groupBy( "userid" ).agg(max("flow_score").as("shop_score"))

    // 支付类APP
    val pay_score = flow_score.where($"appid".isin(pay_appids:_*)).groupBy( "userid" ).agg(max("flow_score").as("pay_score"))

    // 金融类APP
    val fin_score = flow_score.where($"appid".isin(fin_appids:_*)).groupBy( "userid" ).agg(max("flow_score").as("fin_score"))

    val res = flow_score.select("userid","part_type","part_date").distinct()
      .join(shop_score,Seq("userid")).join(pay_score,Seq("userid")).join(fin_score,Seq("userid"))
        .select("userid","shop_score","pay_score","fin_score","part_type","part_date")

    res.write.mode("overwrite").format("parquet")
      .partitionBy("part_type", "part_date")
      .insertInto("suyanli.flow_app_analysis_d")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
