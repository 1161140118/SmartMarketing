package smk.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import utils.DateUtils

object RecoRandomForest {
  val MODEL_PATH = "hdfs:///suyan/chenzhihao/model/rf/"

  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")
    // args0
    val start = param(0)
    // args1
    val end = if (param.length < 2) DateUtils.dateAddAndFormat(start, 6) else param(1) // '-' : 6 days later, or a specific date
    val month = start.substring(0, 6)

//        val start = "20200115"
//        val end = "20200121"
//        val month = "202001"

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


    // 考虑到vip中约50%数据有social_sim，取同样比例target数据
    val target_lim = 2 * ssc.sql(s"select userid from suyanli.reco_sim_input where social_sim>0 and part_type='target' and start_date='$start' and end_date='$end'").count()

    // vip数据过采样
    val vip_count = ssc.sql(s" select *, 1.0 as label from suyanli.reco_sim_input where part_type='vip' and start_date='$start' and end_date='$end' ").count()
    val times = (target_lim/vip_count).toInt
    val vip_ori = ssc.sql(s" select *, 1.0 as label from suyanli.reco_sim_input where part_type='vip' and start_date='$start' and end_date='$end' ")
    var vip_input = vip_ori
    for( i <- (2 to times) ){
      vip_input = vip_input.unionAll(vip_ori)
    }

    //  选取输入数据
    val data = vip_input.unionAll(
      ssc.sql(s" select *, 0.0 as label from suyanli.reco_sim_input where part_type='target' and start_date='$start' and end_date='$end' order by social_sim desc limit $target_lim ")
    ).drop("part_type").drop("start_date").drop("end_date")
    //    data.where("social_sim>0").groupBy("part_type").count()

    //  设定特征属性
    val feature = new VectorAssembler().setInputCols(
      Array("cid","gender","age_sim","act_online_sim","shop_dis","act_weekday_dis","act_weekend_dis","mall_sim","social_sim")
    ).setOutputCol("feature")

    //  变换数据集，特征属性转换为向量
    val data_vec = feature.transform(data).select("userid","feature","label")
    val splited = data_vec.randomSplit(Array(0.8,0.2))
    val train_vec = splited(0)
    val test_vec = splited(1)

//    println( s"Model Coefficient \n " +
//      s"${model.weights.toArray.map( _.toFloat ).mkString(",")}. \n" +
//      s"Model Bias \n " +
//      s"${model.intercept}. \n")

    val indexer = new StringIndexer().setInputCol("label").setOutputCol("category")
    val rf = new RandomForestClassifier()
      .setFeaturesCol("feature")
      .setLabelCol("category")
      .setPredictionCol("predict")
      .setMaxDepth(14)
      .setNumTrees(40)

    // 随机森林 特征重要性
//    val indexer_fitted = indexer.fit(train_vec)
//    val rfmodel = rf.fit( indexer_fitted.transform(train_vec) )
//    rfmodel.featureImportances

    val model = new Pipeline().setStages(Array(indexer,rf)).fit(train_vec)
    val pre = model.transform(test_vec)
//    pre.select("userid","probability","predict","label").show(false)

    // 评估
    val predictionRdd = pre.select("predict","label").rdd.map{
      case Row(predict:Double,label:Double)=>(predict,label)
    }
    val metrics = new MulticlassMetrics(predictionRdd)
    val pre_res = metrics.confusionMatrix.toArray
    val precision = pre_res(3)/(pre_res(2)+pre_res(3))
    val recall = pre_res(3)/(pre_res(1)+pre_res(3))
    val f1 = 2*precision*recall/(precision+recall)
    println(s"RF评估结果：\n " +
            s"分类精确率：${precision}\n " +
            s"分类召回率：${recall}\n " +
            s"F1值：${f1}")

    //    val precision = metrics.precision
    //    val recall = metrics.recall
    //    val weightedPrecision = metrics.weightedPrecision
    //    val weightedRecall= metrics.weightedRecall
    //    val f1 = metrics.weightedFMeasure
//    println(s"RF评估结果：\n " +
//      s"分类准确率：${precision}\n " +
//      s"分类召回率：${recall}\n " +
//      s"加权正确率：${weightedPrecision}\n " +
//      s"加权召回率：${weightedRecall}\n " +
//      s"F1值：${f1}")

    // 保存模型
    sc.parallelize(Seq(model), 1).saveAsObjectFile(MODEL_PATH)

    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")

  }

}
