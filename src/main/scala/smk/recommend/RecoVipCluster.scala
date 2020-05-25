package smk.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import utils.DateUtils

/**
 *  基于vip的聚类
 *
 *
 */
object RecoVipCluster {


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
    // 高忠诚度核心用户聚类表
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_vip_cluster (
        | userid  string,
        | cid     int comment "簇id",
        | gender  string,
        | age     int,
        | loyalty double
        |)
        |COMMENT '高忠诚度核心用户聚类表'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    // 用户聚类及年龄直方图相似度
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_cluster_age_sim (
        | cid int comment "簇id",
        | age int,
        | gender  string,
        | age_sim double  comment "年龄直方图相似度"
        |
        |)
        |COMMENT '用户聚类及年龄直方图相似度'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)
    // 活跃时段直方图相似度
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_act_hour_sim (
        | cid int comment "簇id",
        | act_hour  int comment "流量使用活跃时段",
        | act_sim   double  comment "年龄直方图相似度"
        |
        |)
        |COMMENT '活跃时段直方图相似度'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)
    // 核心商场直方图
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_mall_his (
        | cid int comment "簇id",
        | mall  string comment "商场",
        | mcnt  int  comment "商场访问次数"
        |
        |)
        |COMMENT '核心商场直方图'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)
    // 社交亲密度
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_social_inti (
        | userid string comment "主叫号码",
        | opp_userid  string comment "被叫号码",
        | cid int comment "主叫簇id",
        | social_inti double  comment "亲密度"
        |)
        |COMMENT '核心商场直方图'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)
    // 线上购物活跃度描述
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_shop_des (
        | cid int comment "簇id",
        | shop_avg  double,
        | shop_std  double,
        | null_score  double comment "空值评分"
        |)
        |COMMENT '线上购物活跃度描述'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)
    //  线下访问活跃度描述
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_act_des (
        | cid int comment "簇id",
        | act_weekday_avg  double,
        | act_weekday_std  double,
        | act_weekend_avg  double,
        | act_weekend_std  double
        |)
        |COMMENT '线下访问活跃度描述'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)



    // 获得同时拥有vip与target标记的vip用户
    val vip = ssc.sql(
      s"""
         |select userid
         |from
         |(
         |  select distinct userid, 'target' as part_type from suyanli.mall_target_portrait where start_date like '$month%'
         |  union
         |  select distinct userid, 'vip' as part_type from suyanli.mall_vip_portrait where start_date like '$month%'
         |) t
         |group by userid having count(part_type)=2
         |""".stripMargin)
    vip.registerTempTable("t_vip")

    val cluster_init = ssc.sql(
      s"""
         |select
         |  v.userid as userid, if(gender='1', 'm', 'w') as gender, age,
         |  case when age between 18 and 25 then '18-25'
         |    when age between 26 and 35  then '26-35'
         |    when age between 36 and 45  then '35-45'
         |    when age between 46 and 60  then '46-60'
         |    else  '61-75' end as age_span,
         |  loyalty,
         |  score_pr as pg, rank_l as pg_rank
         |from
         |( select * from t_vip ) v -- 过滤vip用户
         |join
         |( select userid, gender, age from suyanli.basic_info_target_m
         |  where part_month='$month' and part_type='vip' and gender!='3' and age between 18 and 75 ) b on v.userid=b.userid  -- 获得性别年龄信息
         |join
         |( select userid, loyalty  from suyanli.mall_vip_portrait where start_date='$start' and end_date='$end' and loyalty>2 ) l on v.userid=l.userid  -- 获得忠诚度排名
         |join
         |( select userid, score_pr, rank_l from suyanli.social_user_portrait where part_month='$month' and part_type='vip' ) p on v.userid=p.userid  -- 社交影响力
         |
         |""".stripMargin).withColumn("cid", denseRank().over( Window.orderBy("age_span","gender")))  // 标记聚类id
      .withColumn("loy_rank", rowNumber().over(Window.partitionBy("cid").orderBy($"loyalty".desc)))
    val cluster_cnt = cluster_init.groupBy("cid").agg(count("userid").as("cnt")).selectExpr("cid","cast(cnt*0.6 as int) as loy_lim")

    // 得到参与计算的、高忠诚度簇
    val cluster = cluster_init.join(cluster_cnt, Seq("cid")).where("loy_rank<loy_lim").select("userid","cid","gender","age","loyalty","pg","pg_rank")
    cluster.registerTempTable("t_cluster")

    cluster.selectExpr("userid","cid","gender","age","loyalty",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_vip_cluster")


    /**
     *  直方图平滑处理:
     *    分性别，填充个别缺失年龄
     *    对于确实年龄，使用上下相邻年龄的均值填充
     */

    val age_his_df = cluster.groupBy("gender","age").agg(count("userid").as("age_cnt"),first("cid").as("cid"))
      .rdd.groupBy( _.getAs[String]("gender") ).mapValues( v => {
          val get_cid = (gender:String, age:Int) => {
            val a = if (age<=25) 0 else if (age<=35) 1 else if (age<=45) 2 else if (age<=60) 3 else 4
            val g = if(gender=="m") 1 else 2
            a*2 + g
          }
          val gender = v.take(1).toList(0).getAs[String]("gender")
          val rows = v.map( r => Tuple3[Int,Long,Int](r.getInt(1),r.getLong(2),r.getInt(3)) )
            .toList.sortBy( _._1 )
          var rows_new = List[(Int, Long, Int)]()
          var pre = 0l
          var i = 18
          for ( r <- rows  ){
            while(r._1 != i){  // 年龄缺失
              rows_new :+= (i, (pre+r._2)/2, get_cid(gender, i))
              pre = (pre+r._2)/2
              i += 1
            }
            pre = r._2
            i += 1
          }
          rows ::: rows_new
        }).flatMap( r => r._2.map( l => (r._1, l._1, l._2, l._3 ))).toDF("gender","age","age_cnt","cid")
    age_his_df.registerTempTable("t_age_his")

    /**
     *  age 直方图相似度
     *  通过join on age 和 gender， 标定分类与age相似度
     */
    val age_sim = ssc.sql(
      s"""
         |select a.cid as cid, age, gender, (age_cnt-age_cnt_min)/(age_cnt_max-age_cnt_min) as age_sim
         |from
         |( select * from t_age_his ) a
         |join
         |( select cid, max(age_cnt) as age_cnt_max, min(age_cnt) as age_cnt_min from t_age_his group by cid ) b on a.cid=b.cid
         |""".stripMargin)
    age_sim.selectExpr("*",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_cluster_age_sim")

    /**
     *  mall 直方图
     */
    val mall_his = ssc.sql(
      s"""
         |select
         |  cid, mall, sum(mcnt) as mcnt
         |from
         |(
         |  select
         |    v.userid as userid, cid, _c0 as mall, _c1 as mcnt
         |  from
         |    ( select * from t_cluster ) v
         |    join
         |    ( select userid, explode(mall_list)  from suyanli.mall_target_visit_weekly
         |      where start_date='$start' and end_date='$end' ) m
         |    on v.userid = m.userid
         |) t
         |where mall not like '枫叶小镇奥特%' and mall not like '西西里广%' -- 奥特莱斯相关地名
         |group by cid, mall
         |""".stripMargin)
    mall_his.selectExpr("*",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_mall_his")
    //    mall.groupBy("mall").agg(sum("mcnt").as("mcnt")).orderBy($"mcnt".desc).show()

    /**
     *  act_hour  活跃时段直方图相似度
     */
    val act_hour = ssc.sql(
      s"""
         |select cid, act_hour, count(act_hour) as act_cnt
         |from
         |( select userid, act_hour from suyanli.mall_intention_portrait where start_date='$start' and end_date='$end' and part_type='vip' and act_hour>0) a
         |join
         |( select * from t_cluster ) v
         |on v.userid = a.userid
         |group by cid, act_hour
         |""".stripMargin)
    act_hour.registerTempTable("t_act_his")
    val act_hour_sim = ssc.sql(
      s"""
         |select  a.cid as cid, act_hour, (act_cnt-act_min)/(act_max-act_min) as act_sim
         |from
         |( select * from t_act_his ) a
         |join
         |( select cid, max(act_cnt) as act_max, min(act_cnt) as act_min from t_act_his group by cid) b
         |on a.cid=b.cid
         |""".stripMargin)
    act_hour_sim.selectExpr("*",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_act_hour_sim")

    /**
     *  社交亲密度预过滤
     */
    val social_inti = ssc.sql(
      s"""
         |select s.userid, opp_userid, cid, social_inti
         |from
         |( select userid, cid from t_cluster ) c
         |join
         |( select userid, opp_userid, score_dis as social_inti from suyanli.social_mutual_portrait where part_month='$month' ) s
         |on c.userid=s.userid
         |""".stripMargin)
    social_inti.selectExpr("*",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_social_inti")


    /**
     *  购物类APP使用描述
     */
    val stddev = (col: Column) => {  sqrt(avg(col * col) - avg(col) * avg(col)) }
    val shop_score = ssc.sql(
      s"""
         |select
         |  cid, shop_score, if(shop_score is null or shop_score=0, 1, 0) as null_flag
         |from
         |( select userid, shop_score from suyanli.mall_intention_portrait where start_date='$start' and end_date='$end' and part_type='vip') a
         |join
         |( select * from t_cluster ) v on v.userid = a.userid
         |""".stripMargin)
    val shop_des = shop_score.groupBy("cid").agg(avg("shop_score").as("shop_avg"),stddev($"shop_score").as("shop_std")).join(
      shop_score.groupBy("cid").agg( (sum("null_flag")/count("null_flag")).as("null_score") ), Seq("cid")
    )
    shop_des.selectExpr("*",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_shop_des")



    /**
     *  活跃度描述
     */
    val act = ssc.sql(
      s"""
         |select
         |  cid, v.userid as userid, ln(act_weekday+1) as act_weekday, ln(act_weekend+1) as act_weekend
         |from
         |( select userid, nvl(act_weekday,0) as act_weekday, nvl(act_weekend,0) as act_weekend from suyanli.mall_target_portrait where start_date='$start' and end_date='$end' ) a
         |join
         |( select * from t_cluster ) v on v.userid = a.userid
         |""".stripMargin)
    val act_des = act.groupBy("cid").agg(
      avg("act_weekday").as("act_weekday_avg"),
      stddev($"act_weekday").as("act_weekday_std"),
      avg("act_weekend").as("act_weekend_avg"),
      stddev($"act_weekend").as("act_weekend_std")
    )
    act_des.selectExpr("*",s"'$start' as start_date",s"'$end' as end_date").write.mode("overwrite").format("parquet").partitionBy("start_date", "end_date").insertInto("suyanli.reco_act_des")







    //    df.write.mode("overwrite").format("parquet")
//      .partitionBy("start_date", "end_date")
//      .insertInto("suyanli.mall_target_portrait")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}


