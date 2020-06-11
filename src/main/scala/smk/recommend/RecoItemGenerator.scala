package smk.recommend

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import smk.recommend.RecoRandomForest.MODEL_PATH
import utils.DateUtils

object RecoItemGenerator {

  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")
    // args0
    val start = param(0)
    // args1
    val end = if (param.length < 2) DateUtils.dateAddAndFormat(start, 6) else param(1) // '-' : 6 days later, or a specific date

//        val start = "20200115"
//        val end = "20200121"

    println(s"get args: $start, $end . ")
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
        |CREATE TABLE IF NOT  EXISTS suyanli.reco_items (
        | userid  string,
        | probability double  comment "预测概率" ,
        | predict double  comment "预测结果",
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
        |COMMENT '推荐计算输出结果'
        |PARTITIONED BY (
        |    `start_date` string COMMENT '数据开始日期分区 <partition field>',
        |    `end_date` string COMMENT '数据结束日期分区 <partition field>'
        |)
        |stored as parquet
       """.stripMargin)

    val tdata = ssc.sql(s" select * from suyanli.reco_sim_input where part_type='target' and start_date='$start' and end_date='$end' ").drop("part_type").drop("start_date").drop("end_date")
    val feature = new VectorAssembler().setInputCols(
      Array("cid","gender","age_sim","act_online_sim","shop_dis","act_weekday_dis","act_weekend_dis","mall_sim","social_sim")
    ).setOutputCol("feature")
    val tdata_vec =  feature.transform(tdata).select("userid","feature")

    // 从hdfs 加载模型
    val model = sc.objectFile[PipelineModel](MODEL_PATH).first()
    val pre = model.transform(tdata_vec)
    pre.select("userid","probability","predict").show(5,false)

    // join 原始属性
    val tdata_pre =  pre.select( $"userid", $"probability",  $"predict"  ).rdd.map( l => {
      val feature = l.getAs[org.apache.spark.mllib.linalg.DenseVector](1)
      (l.getString(0),feature(1),l.getDouble(2))
      }
    ).toDF("userid","probability","predict").join( tdata, Seq("userid") )

//    tdata_pre.groupBy("predict").count().show()
//    tdata_pre.where(" mall_sim=0 and predict=1").count()

    tdata_pre.withColumn("start_date", lit(start)).withColumn("end_date", lit(end))
      .write.mode("overwrite").format("parquet")
      .partitionBy("start_date", "end_date")
      .insertInto("suyanli.reco_items")

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}