package smk.flow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import utils.DateUtils

/**
 *  <h2>消费偏好画像（日级）：</h2>
 *  <b>注意：依赖前一日</b>
 *  APP 使用情况分析
 *  1.  网购类APP：淘宝等
 *  2.  支付类APP
 *  3.  金融类APP
 *  4.  以上三类使用情况较前一日增加量
 *  5.  APP使用活跃时段
 *
 *  评估策略：
 *  1.  每个APP使用流量对数化，并根据偏离对数均值程度打分: 1-5, 0表示未使用
 *  2.  同类APP使用取打分最高者作为该类APP使用情况得分
 *
 */

object FlowAppPortrait {

  def main(args: Array[String]): Unit = {
    val date = args(0)
    val pre_date = DateUtils.dateAddAndFormat(date,-1)
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
        |CREATE TABLE IF NOT  EXISTS suyanli.flow_app_portrait (
        |  userid  string  comment "手机号",
        |  shop_score double  comment "网购类APP使用得分:0,1-5",
        |  pay_score double  comment "支付类APP使用得分:0,1-5",
        |  fin_score double  comment "金融类APP使用得分:0,1-5",
        |  shop_incr  double,
        |  pay_incr double,
        |  fin_incr double,
        |  act_hour int comment "活跃时段"
        |)
        |COMMENT ' 消费偏好画像 [ 流量使用情况分析日表 ] '
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `part_date` string COMMENT '数据日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

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

    // 打分 1-5
    val flow_score = flow.join(stat, Seq("appid","appname")).selectExpr(
      "userid","appid","appname",
          "case when flow_ln>flow_avg+2*flow_var then 1 when flow_ln<flow_avg-2*flow_var then 5 else (flow_ln-flow_avg)/flow_var +3 end as flow_score ",
          "flow_ln", "part_type", "part_date"
        )

    // 网购类APP
    val shop_score = flow_score.where($"appid".isin(shop_appids:_*)).groupBy( "userid" ).agg(max("flow_score").as("shop_score"))
    // 支付类APP
    val pay_score = flow_score.where($"appid".isin(pay_appids:_*)).groupBy( "userid" ).agg(max("flow_score").as("pay_score"))
    // 金融类APP
    val fin_score = flow_score.where($"appid".isin(fin_appids:_*)).groupBy( "userid" ).agg(max("flow_score").as("fin_score"))

    flow_score.select("userid","part_type").distinct().registerTempTable("t_flow")
    shop_score.registerTempTable("t_shop")
    pay_score.registerTempTable("t_pay")
    fin_score.registerTempTable("t_fin")

    // 活跃时段
    val act = ssc.sql(
      s"""
         |select
         |  userid, hour, flow, part_type
         |from
         |(
         |  select
         |    userid, hour, sum(flow) as flow, part_type,
         |    row_number() over(partition by userid order by sum(flow) desc) as rank
         |  from suyanli.flow_app_stat_h
         |  where part_date = '$date'
         |    and appid like 'C%'
         |  group by userid, hour, part_type
         |) t
         |where rank=1
         |""".stripMargin)
    act.registerTempTable("t_act")

    val score = ssc.sql(
      s"""
         |select
         |  coalesce(t.userid, d.userid) as userid,
         |  nvl(s.shop_score, 0) as shop_score,
         |  nvl(p.pay_score, 0) as pay_score,
         |  nvl(f.fin_score, 0) as fin_score,
         |  if(d.shop_score=0, 0, nvl(s.shop_score - d.shop_score , 0)) as shop_incr,
         |  if(d.pay_score=0, 0, nvl(p.pay_score - d.pay_score , 0)) as pay_incr,
         |  if(d.fin_score=0, 0, nvl(f.fin_score - d.fin_score , 0)) as fin_incr,
         |  nvl(t.hour, -1) as act_hour,
         |  coalesce(t.part_type, d.part_type) as part_type,
         |  '$date' as part_date
         |from
         |(
         |  select 
         |    coalesce(flow.userid, act.userid) as userid,
         |    coalesce(flow.part_type, act.part_type) as part_type,
         |    act.hour as hour
         |  from
         |    ( select * from t_flow ) flow
         |    full join
         |    ( select * from t_act ) act
         |    on flow.userid=act.userid and flow.part_type=act.part_type
         |) t
         |left join
         |( select * from t_shop ) s on t.userid=s.userid
         |left join
         |( select * from t_pay ) p  on t.userid = p.userid
         |left join
         |( select * from t_fin ) f  on t.userid = f.userid
         |full join
         |( select * from suyanli.flow_app_portrait where part_date = '$pre_date' ) d
         |  on t.userid=d.userid and t.part_type=d.part_type
         |""".stripMargin).cache()

    score.write.mode("overwrite").format("parquet")
      .partitionBy("part_type", "part_date")
      .insertInto("suyanli.flow_app_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
