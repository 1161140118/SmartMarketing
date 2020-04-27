package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
 * 用户语音按基站月表：
 *  1. 筛选哈尔滨
 *  2. 按 serv_no 聚集，避免基站不同的影响
 *  2. 计算通话时长描述属性和通话次数描述属性
 *  3. 计算上述属性的对数正态分布的均值与方差
 *  4. 根据上述均值和方差，判定用户特殊性（有效性）：
 *     对 calling_avg_ln, called_avg_ln 使用 4σ 标准
 *     对 calling_cnt_ln, called_cnt_ln, one_min_calling_cnt_ln 使用 3σ 标准
 *  5. 按月份和有效性，分区存储
 */

object SocialGsmStatM {

  def main(args: Array[String]): Unit = {
    val month = args(0)

    println(s"get args: $month .")
    val t1 = System.currentTimeMillis()

    val conf = new SparkConf()
      .set("spark.executor.instances","4")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","16G")
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


    val df_group = ssc.sql(
      s"""
         |select 
         |  *,
         |  calling_avg / calling_cnt as calling_univ,
         |  ln(calling_avg/calling_cnt) as calling_univ_ln,
         |  ln(calling_avg +1) as calling_avg_ln,
         |  ln(called_avg +1) as called_avg_ln,
         |  ln(calling_cnt +1) as calling_cnt_ln,
         |  ln(called_cnt +1) as called_cnt_ln,
         |  ln(one_min_calling_cnt +1) as one_min_calling_cnt_ln,
         |  ln(loc_cnt +1) as loc_cnt_ln
         |from
         |(
         |  select
         |    serv_no as userid,
         |    loc_cnt,
         |    calling_dur / calling_cnt as calling_avg,
         |    called_dur / called_cnt as called_avg,
         |    calling_cnt, called_cnt,
         |    one_min_calling_cnt,
         |    local_call_cnt, toll_call_cnt, roam_call_cnt
         |  from
         |  (
         |    select
         |      serv_no,
         |      count(*) as loc_cnt,
         |      sum(calling_dur) as calling_dur,
         |      sum(called_dur) as called_dur,
         |      sum(calling_cnt) as calling_cnt,
         |      sum(called_cnt) as called_cnt,
         |      sum(one_min_calling_cnt) as one_min_calling_cnt,
         |      sum(local_call_cnt) as local_call_cnt,
         |      sum(toll_call_cnt) as toll_call_cnt,
         |      sum(roam_call_cnt) as roam_call_cnt
         |    from suyanli.ti_ub_gsm_bs_m
         |    where part_month = '$month'
         |      and serv_no between '1' and '2'
         |      and home_region_code=='451'
         |    group by serv_no
         |  ) t
         |) tt
         |""".stripMargin)

    val df_des = df_group.describe("calling_avg_ln","called_avg_ln","calling_univ_ln","calling_cnt_ln","called_cnt_ln", "one_min_calling_cnt_ln", "loc_cnt_ln")

    val mean = df_des.where("summary = 'mean' ").collect()(0)
    val stddev = df_des.where("summary = 'stddev' ").collect()(0)

    /**
     * 对 calling_avg_ln, called_avg_ln 使用 4σ 标准, 且超过 calling_cnt_ln 和 called_cnt_ln 的 1σ 限制
     * 对 calling_univ_ln, calling_cnt_ln, called_cnt_ln, one_min_calling_cnt_ln 使用 3σ 标准
     */

    val df = df_group.withColumn(
      "part_valid",
      when(
        // calling_avg_ln
        ( $"calling_avg_ln".isNotNull
          && $"calling_avg_ln" > mean.getString(1).toDouble+ 3*stddev.getString(1).toDouble
          && $"calling_cnt_ln" > mean.getString(4).toDouble + stddev.getString(4).toDouble
          )
          // called_avg_ln
          .or(  $"called_avg_ln".isNotNull
            && $"called_avg_ln" > mean.getString(2).toDouble+ 3*stddev.getString(2).toDouble
            && $"called_cnt_ln" > mean.getString(5).toDouble + stddev.getString(5).toDouble
          )
          // calling_univ_ln
          .or(  $"calling_univ_ln".isNotNull
            && $"calling_univ_ln" < mean.getString(3).toDouble - 3*stddev.getString(3).toDouble
          )
          .or( $"calling_cnt_ln" > mean.getString(4).toDouble+ 3*stddev.getString(4).toDouble )
          .or( $"called_cnt_ln" > mean.getString(5).toDouble+ 3*stddev.getString(5).toDouble )
          .or( $"one_min_calling_cnt_ln" > mean.getString(6).toDouble+ 3*stddev.getString(6).toDouble)
          .or( $"loc_cnt_ln" > mean.getString(7).toDouble+ 3*stddev.getString(7).toDouble) ,
        false)
        .otherwise(true)
    ).withColumn("part_month", lit(month))

    df.select("userid", "loc_cnt", "calling_avg", "called_avg", "calling_cnt", "called_cnt", "calling_univ",
      "one_min_calling_cnt", "local_call_cnt", "toll_call_cnt", "roam_call_cnt",
      "part_month", "part_valid")
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_month","part_valid").insertInto("suyanli.social_gsm_stat_m")


    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")


    /**
     * 查看分布

    df_group.selectExpr(
      "sum( case when calling_avg_ln>4.3 then 1 else 0 end) as calling_avg_ln_0 ",
      "sum( case when calling_avg_ln>5.0 then 1 else 0 end) as calling_avg_ln_1",
      "sum( case when calling_avg_ln>5.7 then 1 else 0 end) as calling_avg_ln_2",
      "sum( case when calling_avg_ln>6.5 then 1 else 0 end) as calling_avg_ln_3",
      "sum( case when calling_avg_ln>7.2 then 1 else 0 end) as calling_avg_ln_4",
      "sum( case when calling_cnt_ln>3.2 then 1 else 0 end) as calling_cnt_ln_0 ",
      "sum( case when calling_cnt_ln>4.6 then 1 else 0 end) as calling_cnt_ln_1",
      "sum( case when calling_cnt_ln>6.0 then 1 else 0 end) as calling_cnt_ln_2",
      "sum( case when calling_cnt_ln>7.4 then 1 else 0 end) as calling_cnt_ln_3",
      "sum( case when calling_cnt_ln>8.8 then 1 else 0 end) as calling_cnt_ln_4"
    ).show

    df_group.selectExpr(
      "sum( case when one_min_calling_cnt_ln>2.7 then 1 else 0 end) as one_min_ln_0 ",
      "sum( case when one_min_calling_cnt_ln>4.1 then 1 else 0 end) as one_min_ln_1",
      "sum( case when one_min_calling_cnt_ln>5.4 then 1 else 0 end) as one_min_ln_2",
      "sum( case when one_min_calling_cnt_ln>6.8 then 1 else 0 end) as one_min_ln_3",
      "sum( case when one_min_calling_cnt_ln>8.1 then 1 else 0 end) as one_min_ln_4"
    ).show

    df_group.selectExpr(
      "sum( case when loc_cnt_ln>2.7 then 1 else 0 end) as loc_cnt_ln_0 ",
      "sum( case when loc_cnt_ln>3.8 then 1 else 0 end) as loc_cnt_ln_1 ",
      "sum( case when loc_cnt_ln>4.8 then 1 else 0 end) as loc_cnt_ln_2 ",
      "sum( case when loc_cnt_ln>5.8 then 1 else 0 end) as loc_cnt_ln_3 ",
      "sum( case when loc_cnt_ln>6.8 then 1 else 0 end) as loc_cnt_ln_4 "
    ).show

    df_group.selectExpr(
      "sum( case when calling_univ_ln<1.06 then 1 else 0 end) as calling_univ_ln_0 ",
      "sum( case when calling_univ_ln<-0.4 then 1 else 0 end) as calling_univ_ln_1 ",
      "sum( case when calling_univ_ln<-1.9 then 1 else 0 end) as calling_univ_ln_2 ",
      "sum( case when calling_univ_ln<-3.4 then 1 else 0 end) as calling_univ_ln_3 ",
      "sum( case when calling_univ_ln<-4.9 then 1 else 0 end) as calling_univ_ln_4 "
    ).show

    */

  }

}
