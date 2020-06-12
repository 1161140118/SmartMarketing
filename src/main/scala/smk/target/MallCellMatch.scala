package smk.target

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 根据localname中商场坐标信息，筛选到达过商场的用户
  *
  * 商场 - 基站 匹配
  */
object MallCellMatch {

  def main(args: Array[String]): Unit = {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName(getClass.getName.init))
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._

    val mins = ssc.sql(
      s"""
         |  select
         |    lng, lat, store,
         |    case area when '中心' then 'center'
         |      when '师大' then 'shida'
         |      when '松北' then 'songbei' end as area,
         |    round(sqrt(dis2)*1000, 2) as dis
         |  from
         |  -- 为每个商场预匹配最近的不超过5个基站坐标
         |  -- 双向选择避免同一个基站匹配多个mall
         |  (
         |    select
         |      lng, lat, store, area, dis2,
         |      row_number() over(partition by store, area order by dis2) as rankid
         |    from
         |    -- 为每个基站匹配最近的mall
         |    (
         |      select
         |        *,  row_number() over(partition by lng, lat order by dis2) as rankid
         |      from
         |      -- 过滤距离, dis单位 平方千米
         |      (
         |        select
         |          b.lng as lng, b.lat as lat, store, area,
         |          pow( (a.lng-b.lng)*77.521, 2) + pow( (a.lat-b.lat)*111.195, 2) as dis2
         |        from
         |        (  select store, lng, lat, area  from suyanli.localname_main_mall  ) a
         |        join
         |        (  select lng, lat  from suyanli.dm_cell_info  where city='2301'   ) b
         |      ) t1 -- 基本信息宽表
         |      -- dis单位 平方千米, 0.01 即筛选0.1km内
         |      -- 先保留300m内的基站
         |      where dis2 < 0.09
         |    ) t2
         |    where rankid = 1
         |  ) t3
         |  where rankid <= 5
         |""".stripMargin).cache()

    // 根据距离局部过滤基站
    val scope = mins.rdd.groupBy( r=> ( r.getString(2), r.getString(3) ))
      .mapValues( v => {
        val cells = v.map( r => Tuple2[Double,Row](r.getDouble(4), r))
          .toList.sortBy( _._1 )
        var res = List[Row]()
        var scope = 300 // 初始化300m范围
        for ( cell <- cells ){
          if (cell._1 < scope ){
            // 过滤 scope
            if (cell._1 <= 100){
              res :+= cell._2
              if(res.length>1) scope = 100  // 100m内有2个以上，限制scope
            } else if (cell._1 <= 180){
              res :+= cell._2
              if(res.length>0) scope = 180  // 180m内有，限制scope
            } else {
              res :+= cell._2
            }
          }
        }
        res.map( r=>(r.getFloat(0),r.getFloat(1),r.getString(2),r.getString(3),r.getDouble(4)) )
      }).flatMap( _._2 ).toDF("lng","lat","store","area","dis")

    scope.registerTempTable("t_scope_match")

    // 补充枫叶小镇基站记录
    val mall = "枫叶小镇奥特莱斯"
    val outlets = ssc.sql(
      s"""
         |select baseid, cast(lng as float), cast(lat as float), '$mall' as store, 0 as store_id, 'songbei' as area, 0 as dis
         |from ( select * from suyanli.mall_cell_info ) t
         |""".stripMargin)

    val mall_cell =  ssc.sql(
      s"""
         |select
         |  baseid, m.lng as lng, m.lat as lat, store,
         |  dense_rank() over (order by store) as store_id,
         |  area, dis
         |from
         |( select * from t_scope_match ) m
         |join
         |( select baseid, lng, lat from suyanli.dm_cell_info where city='2301' ) c
         |on m.lng = c.lng and m.lat = c.lat
         |where store!='$mall'
       """.stripMargin).unionAll(outlets).distinct()

    mall_cell.write.saveAsTable("suyanli.mall_cell_match")

    sc.stop()

  }

}
