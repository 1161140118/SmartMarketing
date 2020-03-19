package smk.target

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 根据localname中商场坐标信息，筛选到达过商场的用户
  *
  * 商场 - 基站 匹配
  */
object MallCellMatch {

  def main(args: Array[String]): Unit = {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName( getClass.getName.init ))
    val ssc:SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    val mall_cell =  ssc.sql(
      s"""
         |select
         |  baseid, lng, lat, store, area, round(sqrt(dis2)*1000, 2) as dis
         |from
         |-- 为每个商场匹配最近的不超过30个基站
         |-- 双向选择避免同一个基站匹配多个mall
         |(
         |  select
         |    baseid, lng, lat, store, area, dis2,
         |    row_number() over(partition by store, area order by dis2) as rankid
         |  from
         |  -- 为每个基站匹配最近的mall
         |  (
         |    select
         |      *,
         |      row_number() over(partition by baseid, lng, lat, store, area order by dis2) as rankid
         |    from
         |    -- 过滤距离
         |    (
         |      select
         |        baseid, b.lng as lng, b.lat as lat, store, area,
         |        pow( (a.lng-b.lng)*77.521, 2) + pow( (a.lat-b.lat)*111.195, 2) as dis2
         |      from
         |      (
         |        select store, lng, lat, area
         |        from suyanli.localname_main_mall
         |      ) a
         |      join
         |      (
         |        select baseid, lng, lat
         |        from suyanli.dm_cell_info
         |        where city='2301'
         |      ) b
         |    ) t1 -- 基本信息宽表
         |    where dis2 < 0.01
         |  ) t2
         |  where rankid = 1
         |) t3
         |where rankid <= 30
       """.stripMargin)

    mall_cell.write.saveAsTable("suyanli.mall_cell_match")

    sc.stop()

  }

}
