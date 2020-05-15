package smk.target

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 2. 计算用户访问浏览信息
  *    1. 对每个基站，根据地理坐标，绑定半径100米距离内的最近的商场，成功绑定的这部分基站作为有效基站
  *       限制每个商场附近最多绑定30个最近的基站（outlets附近共25个）
  *       注：1经度大约77.521km(北纬45.8度附近), 纬度1度大约111.195km，
  *    2. 用户连接有效基站即达到商场附近，连接记录标定商场名称
  *    3. 对每条用户访问记录，标定商圈名
  */
object TargetTraceInfo {

  def main(args: Array[String]): Unit = {
    val start = args(0)
    val end = args(1)
    val t1 = System.currentTimeMillis()
    println(s"get args: $start, $end.")

    val conf = new SparkConf()
      .set("spark.executor.instances","8")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","8G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    ssc.sql(
      s"""
         |select
         |  userid, dura_sec, store as mall, area, startt, endt, level, part_date
         |from
         |(
         |  select
         |    userid, baseid,
         |    (endt - startt)/1000 as dura_sec,
         |    startt, endt,
         |    level,
         |    part_date
         |  from suyanli.s1mm_trace
         |  where part_date between '$start' and '$end'
         |    -- 为了缩减数据规模，提前过滤数据
         |    and length(userid)=11
         |    and from_unixtime(startt / 1000, 'HH') between '09' and '21' -- 取小时，限制到21
         |    and from_unixtime(endt / 1000, 'HH')   between '09' and '21'
         |    and (endt - startt)/1000 between 30 and 7200  --  时间跨度限制在半分钟到2小时
         |) a
         |join
         |( select baseid, store, area from suyanli.mall_cell_match ) b
         |on a.baseid = b.baseid
       """.stripMargin)
      .write.mode("overwrite").format("parquet").partitionBy("part_date")
      .insertInto("suyanli.mall_target_trace_info")

    sc.stop()

    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
