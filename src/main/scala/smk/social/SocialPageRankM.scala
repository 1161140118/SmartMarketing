package smk.social

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions.lit

/**
 *  计算PageRank和入度与出度
 */
object SocialPageRankM {

  def main(args: Array[String]): Unit = {
    val month = args(0)
    val iter = args(1).toInt

    println(s"get args: $month, $iter .")

    val conf = new SparkConf()
      .set("spark.executor.instances","4")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","16G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val t1 = System.currentTimeMillis()

    val hc = new HiveContext(sc)
    hc.sql(
      """
        |CREATE TABLE IF NOT  EXISTS suyanli.social_pagerank_m (
        |  userid  string  comment "主叫方手机号",
        |  score  double  comment "PageRank 得分",
        |  rank int comment "PageRank 排名",
        |  in_d int comment "入度",
        |  out_d int  comment "出度"
        |)
        |COMMENT ' 社交影响力PageRank排名  '
        |PARTITIONED BY (
        |    `part_month` string COMMENT '数据月份分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val circle = ssc.sql(
      s"""
         |select cast(userid as Long) , cast(opp_userid as Long)
         |from suyanli.social_gsm_circle_info_m
         |where part_month='$month' and calling_dur >0 and calling_cnt>0
         |""".stripMargin)

    // 获得所有顶点
    val vertexDf = circle.select("userid").unionAll(circle.selectExpr("opp_userid as userid")).distinct()

    // 构建顶点rdd、边rdd、社交关系图
    val vertices = vertexDf.rdd.map( row => (row.getLong(0),1) )
    val edges = circle.rdd.map( row => Edge(row.getLong(0), row.getLong(1), 1) )
    val graph = Graph(vertices,edges)

    // 计算入度、出度
    graph.inDegrees.toDF("userid","in_degree").registerTempTable("t_in_degree")
    graph.outDegrees.toDF("userid","out_degree").registerTempTable("t_out_degree")

    // 计算 pagerank
    val rank = graph.staticPageRank(iter)
    rank.vertices.toDF("userid", "score").registerTempTable("t_pagerank")

    val res = ssc.sql(
      s"""
         |select
         |  cast(t.userid as string) as userid, score,
         |  row_number() over (order by score desc ) as rank,
         |  nvl(in_degree, 0) as in_degree,
         |  nvl(out_degree, 0) as out_degree
         |from
         |(
         |  select cast(userid as long), score
         |  from t_pagerank
         |) t
         |left join
         |( select userid, in_degree from t_in_degree ) a on t.userid = a.userid
         |left join
         |( select userid, out_degree from t_out_degree ) b on t.userid = b.userid
         |order by rank
         | """.stripMargin).cache()

    res.withColumn("part_month", lit(month))
      .write.mode("overwrite").format("parquet")
      .partitionBy("part_month").insertInto("suyanli.social_pagerank_m")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1)/60000} min.")

  }

}
