package smk.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import utils.DateUtils

/**
 *  目标客户特征/相似度属性 构建
 */
object RecSimCalculate {

  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")
    // args0
    val start = param(0)
    // args1
    val end = if ( param.length < 2 ) DateUtils.dateAddAndFormat(start,6) else param(1) // '-' : 6 days later, or a specific date
    val month = start.substring(0,6)

    println(s"get args: $start, $end . more: $month ")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances", "4")
      .set("spark.executor.cores", "2")
      .set("spark.executor.memory", "8G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val hc = new HiveContext(sc)
    // 用户聚类及年龄直方图相似度
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_sim_input (
        | userid  string,
        | cid int comment "簇id",
        | gender  int,
        | age_sim double  comment "年龄直方图相似度",
        | act_online_sim  double,
        | shop_dis  double,
        | act_weekday_dis double,
        | act_weekend_dis double,
        | mall_sim  double,
        | social_sim  double
        |)
        |COMMENT '推荐计算输入相似度属性'
        |PARTITIONED BY (
        |    `part_type`  string COMMENT '客户类型分区: vip / target <partition field>',
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val max2 = (a:Int, b:Int) => ( if(a>b) a else b)
    ssc.udf.register("max2", max2)

    /**
     *  vip
     */

    // 核心客户基本信息和部分属性
    val vip_info = ssc.sql(
      s"""
         |select
         |  v.userid as userid, v.cid as cid, age, nvl(act_hour,-1) as act_hour, nvl(shop_score,0) as shop_score, nvl(act_weekday,0) as act_weekday, nvl(act_weekend,0) as act_weekend
         |from
         |( select userid, cid, gender, age from suyanli.reco_vip_cluster where start_date='$start' and end_date='$end') v
         |left join
         |( select userid, max(shop_score) as shop_score, max(act_hour) as act_hour from suyanli.mall_intention_portrait where start_date='$start' and end_date='$end' and part_type='vip' group by userid ) h on v.userid=h.userid
         |left join
         |( select userid, ln(act_weekday+1) as act_weekday, ln(act_weekend+1) as act_weekend from suyanli.mall_target_portrait where start_date='$start' and end_date='$end' ) w on v.userid=w.userid
         |""".stripMargin)
    vip_info.registerTempTable("t_vip_info")

    // 核心客户簇相似度属性计算
    val vip_score_sim = ssc.sql(
      s"""
         |select
         |  userid, v.cid as cid, nvl(age_sim,0) as age_sim, nvl(act_sim,0) as act_online_sim,
         |  if( shop_score=0 or shop_score is null, 1-null_score, (shop_score-shop_avg)/(3*shop_std) ) as shop_dis,
         |  (nvl(act_weekday,0)-act_weekday_avg)/(3*act_weekday_std) as act_weekday_dis,
         |  (nvl(act_weekend,0)-act_weekend_avg)/(3*act_weekend_std) as act_weekend_dis
         |from
         |( select * from t_vip_info ) v
         |left join
         |-- 年龄相似度
         |( select cid, age, age_sim from suyanli.reco_cluster_age_sim where start_date='$start' and end_date='$end' ) a on v.cid=a.cid and v.age=a.age
         |left join
         |-- 网购相似度
         |( select * from suyanli.reco_shop_des where start_date='$start' and end_date='$end' ) s on v.cid=s.cid
         |left join
         |-- 线下活跃度相似度
         |( select * from suyanli.reco_act_des where start_date='$start' and end_date='$end' ) f on v.cid=f.cid
         |left join
         |-- 线上活跃时段相似度
         |( select * from suyanli.reco_act_hour_sim where start_date='$start' and end_date='$end' ) o on v.cid=o.cid and v.act_hour=o.act_hour
         |""".stripMargin)
    vip_score_sim.registerTempTable("t_vip_score_sim")

    // vip_info.agg(countDistinct("userid"),count("userid")).show()
    // vip_info.groupBy("userid").agg(count("userid").as("cnt")).where("cnt>1").show()

    // 核心客户 核心商场相似度计算
    val vip_mall_match = ssc.sql(
      s"""
         |select
         |  userid, t.cid as cid,  t.mall as mall, t.mcnt as mcnt, h.mcnt as vcnt
         |from
         |(
         |  select  v.userid as userid, cid, _c0 as mall, _c1 as mcnt
         |  from
         |    ( select * from t_vip_info ) v
         |    join
         |    ( select userid, explode(mall_list)  from suyanli.mall_target_visit_weekly  where start_date='$start' and end_date='$end' ) m
         |    on v.userid = m.userid
         |  where _c0 not like '枫叶小镇奥特%' and _c0 not like '西西里广%' -- 奥特莱斯相关地名
         |) t
         |left join
         |( select cid, mall, mcnt from suyanli.reco_mall_his where start_date='$start' and end_date='$end' ) h on t.cid=h.cid and t.mall=h.mall
         |""".stripMargin)
    // vip_mall_sim.where(" vmall is null ").show()
    val vip_mall_sim = vip_mall_match.groupBy("userid").agg( (sum($"mcnt"*$"vcnt")/sqrt( sum(pow($"mcnt",2)))).as("mall_sim"))
//    val vip_mall_sim_des = vip_mall_sim.describe("mall_sim")
//    vip_mall_sim_des.show()
    vip_mall_sim.registerTempTable("t_vip_mall_sim")

    // 核心客户 社交相似度计算
    val vip_social_sim = ssc.sql(
      s"""
         |select
         |  userid, max(scoial_inti) as social_sim
         |from
         |( select * from t_vip_info ) v
         |join
         |( select opp_userid, scoial_inti from suyanli.reco_social_inti where start_date='$start' and end_date='$end') s on v.userid = s.opp_userid
         |group by userid
         |""".stripMargin)
//    vip_social_sim.describe("social_sim").show()
    vip_social_sim.registerTempTable("t_vip_social_sim")

    val vip_sim = ssc.sql(
      s"""
         |select
         |  t.userid as userid, t.cid as cid,
         |  if(cid%2=1,1,2) as gender, age_sim, act_online_sim, shop_dis, act_weekday_dis, act_weekend_dis,
         |  nvl(ln(mall_sim+1),0) as mall_sim,
         |  nvl(social_sim,0) as social_sim
         |from
         |( select * from t_vip_score_sim ) t
         |left join
         |( select * from t_vip_mall_sim  ) m on t.userid = m.userid
         |left join
         |( select * from t_vip_social_sim  ) s on t.userid = s.userid
         |""".stripMargin)
//    vip_sim.show()
//    vip_sim.describe("mall_sim","social_sim")

    vip_sim.withColumn("part_type",lit("vip"))
      .withColumn("start_date", lit(start))
      .withColumn("end_date", lit(end))
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_type","start_date", "end_date")
      .insertInto("suyanli.reco_sim_input")

    /**
     *  target
     */

    val target_info = ssc.sql(
      s"""
         |select
         |  t.userid as userid, cid, b.age as age, age_sim, nvl(act_hour,-1) as act_hour, nvl(shop_score,0) as shop_score, nvl(act_weekday,0) as act_weekday, nvl(act_weekend,0) as act_weekend
         |from
         |( select userid, ln(max(act_weekday)+1) as act_weekday, ln(max(act_weekend)+1) as act_weekend
         |  from suyanli.mall_target_portrait where start_date='$start' and end_date='$end' group by userid having count(*)=1 ) t
         |join
         |( select userid, if(gender='1','m','w') as gender, age from suyanli.basic_info_target_m
         |  where part_month='$month' and part_type='target' and gender!='3' and age between 18 and 75 ) b on t.userid=b.userid  -- 获得性别年龄信息
         |join
         |( select cid, age, gender, age_sim from suyanli.reco_cluster_age_sim where start_date='$start' and end_date='$end' ) c
         |  on b.age=c.age and b.gender=c.gender
         |join
         |( select userid, max(shop_score) as shop_score, max(act_hour) as act_hour from suyanli.mall_intention_portrait
         |  where start_date='$start' and end_date='$end' and part_type='target' group by userid ) h on t.userid=h.userid
         |""".stripMargin)
    target_info.registerTempTable("t_target_info")
    // target_info.agg(countDistinct("userid"),count("userid")).show()

    val target_score_sim = ssc.sql(
      s"""
         |select
         |  userid, t.cid as cid, age_sim, nvl(act_sim,0) as act_online_sim,
         |  if( shop_score=0 or shop_score is null, 1-null_score, (shop_score-shop_avg)/(3*shop_std) ) as shop_dis,
         |  (nvl(act_weekday,0)-act_weekday_avg)/(3*act_weekday_std) as act_weekday_dis,
         |  (nvl(act_weekend,0)-act_weekend_avg)/(3*act_weekend_std) as act_weekend_dis
         |from
         |( select * from t_target_info ) t
         |join
         |-- 网购相似度
         |( select * from suyanli.reco_shop_des where start_date='$start' and end_date='$end' ) s on t.cid=s.cid
         |join
         |-- 线下活跃度相似度
         |( select * from suyanli.reco_act_des where start_date='$start' and end_date='$end' ) f on t.cid=f.cid
         |join
         |-- 线上活跃时段相似度
         |( select * from suyanli.reco_act_hour_sim where start_date='$start' and end_date='$end' ) o on t.cid=o.cid and t.act_hour=o.act_hour
         |""".stripMargin)
    target_score_sim.registerTempTable("t_target_score_sim")

    val target_mall_match = ssc.sql(
      s"""
         |select
         |  userid, t.cid as cid,  t.mall as mall, t.mcnt as mcnt, h.mcnt as vcnt
         |from
         |(
         |  select  v.userid as userid, cid, _c0 as mall, _c1 as mcnt
         |  from
         |    ( select * from t_target_info ) v
         |    join
         |    ( select userid, explode(mall_list)  from suyanli.mall_target_visit_weekly  where start_date='$start' and end_date='$end' ) m
         |    on v.userid = m.userid
         |  where _c0 not like '枫叶小镇奥特%' and _c0 not like '西西里广%' -- 奥特莱斯相关地名
         |) t
         |left join
         |( select cid, mall, mcnt from suyanli.reco_mall_his where start_date='$start' and end_date='$end' ) h on t.cid=h.cid and t.mall=h.mall
         |""".stripMargin)
//    target_mall_match.show()
    val target_mall_sim = target_mall_match.groupBy("userid").agg( (sum($"mcnt"*$"vcnt")/sqrt( sum(pow($"mcnt",2)))).as("mall_sim"))
    target_mall_sim.registerTempTable("t_target_mall_sim")
    // target_mall_sim.where(" vcnt is null ").show()

    val target_social_sim = ssc.sql(
      s"""
         |select
         |  userid, max(scoial_inti) as social_sim
         |from
         |( select * from t_target_info ) t
         |join
         |( select opp_userid, scoial_inti from suyanli.reco_social_inti where start_date='$start' and end_date='$end') s on t.userid = s.opp_userid
         |group by userid
         |""".stripMargin)
//    target_social_sim.describe("social_sim").show()
    target_social_sim.registerTempTable("t_target_social_sim")

    val target_sim = ssc.sql(
      s"""
         |select
         |  t.userid as userid, t.cid as cid,
         |  if(cid%2=1,1,2) as gender, age_sim, act_online_sim, shop_dis, act_weekday_dis, act_weekend_dis,
         |  nvl(ln(mall_sim+1),0) as mall_sim,
         |  nvl(social_sim,0) as social_sim
         |from
         |( select * from t_target_score_sim ) t
         |left join
         |( select * from t_target_mall_sim  ) m on t.userid = m.userid
         |left join
         |( select * from t_target_social_sim  ) s on t.userid = s.userid
         |""".stripMargin)
//    target_sim.show()
//    target_sim.describe("mall_sim","social_sim")

    target_sim.withColumn("part_type",lit("target"))
      .withColumn("start_date", lit(start))
      .withColumn("end_date", lit(end))
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_type","start_date", "end_date")
      .insertInto("suyanli.reco_sim_input")


    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")


  }

}
