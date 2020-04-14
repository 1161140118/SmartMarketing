package smk.basicInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object BasicInfoMonthly {

  def main(args: Array[String]): Unit = {
    val date = args(0)
    val month = date.substring(0,6)
    println(s"get args: ${args(0)}, $month .")
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
         |CREATE TABLE IF NOT  EXISTS suyanli.basic_info_m (
         |  userid  string  comment "手机号",
         |  user_id string  comment "用户标识",
         |  cust_id string  comment "客户标识",
         |  gender string,
         |  age int,
         |  birth_date  string,
         |  nation  string  comment "国籍",
         |  language  string  comment "语言"
         |)
         |COMMENT '个人用户基本信息月表 - 用户基本信息月表&个人客户基本信息日表'
         |PARTITIONED BY (
         |    `part_month` string COMMENT '数据月份分区 <partition field>'
         |)
         |stored as parquet
       """.stripMargin)

    val df = ssc.sql(
      s"""
         |select
         |  userid,
         |  user_id,
         |  a.cust_id as cust_id,
         |  nvl(a.gender, b.gender) as gender,
         |  nvl(a.age, b.age) as age,
         |  nvl(a.birth_date, b.birth_month) as birth_date,
         |  nation,
         |  language,
         |  '$month' as part_month
         |from
         |(
         |  select * from
         |  (
         |    select
         |      serv_no as userid,
         |      cast(user_id as string) as user_id,
         |      cast(cust_id as string) as cust_id,
         |      county_code,
         |      cust_gender as gender,
         |      cust_age as age,
         |      cust_birth_date as birth_date,
         |      row_number() over (partition by serv_no order by user_id desc) as rankid
         |    from suyanli.ti_ue_basic_info_m
         |    where part_month = '$month'
         |      and city_code = '2301'
         |      and serv_no between '1' and '2'   -- filter enterprise
         |      and length(serv_no)=11
         |  ) t
         |  where rankid=1
         |) a
         |join
         |(
         |  select cust_id, cust_addr, nation, birth_month, language, gender, age
         |  from suyanli.ti_ib_indiv_base_d
         |  where part_date = '$date'
         |) b
         |on a.cust_id = b.cust_id
       """.stripMargin)

    df.write.mode("overwrite").format("parquet").partitionBy("part_month").insertInto("suyanli.basic_info_m")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
