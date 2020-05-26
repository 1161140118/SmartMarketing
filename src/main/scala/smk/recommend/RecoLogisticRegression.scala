package smk.recommend

import org.apache.spark.ml.Pipeline
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import utils.DateUtils

import scala.collection.mutable

object RecoLogisticRegression {


  def main(args: Array[String]): Unit = {
    val param = args(0).split(" ")
    // args0
    val start = param(0)
    // args1
    val end = if (param.length < 2) DateUtils.dateAddAndFormat(start, 6) else param(1) // '-' : 6 days later, or a specific date
    val month = start.substring(0, 6)

//    val start = "20200115"
//    val end = "20200121"
//    val month = "202001"

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

    //  选取输入数据
    val data = ssc.sql(s" select *, 1.0 as label from suyanli.reco_sim_input where part_type='vip' and start_date='$start' and end_date='$end' ").unionAll(
      ssc.sql(s" select *, 0.0 as label from suyanli.reco_sim_input where part_type='target' and start_date='$start' and end_date='$end' order by social_sim desc limit $target_lim ").sample(false,0.1)
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

    /**
     * LR建模
     * setMaxIter设置最大迭代次数(默认100),具体迭代次数可能在不足最大迭代次数停止(见下一条)
     * setTol设置容错(默认1e-6),每次迭代会计算一个误差,误差值随着迭代次数增加而减小,当误差小于设置容错,则停止迭代
     * setRegParam设置正则化项系数(默认0),正则化主要用于防止过拟合现象,如果数据集较小,特征维数又多,易出现过拟合,考虑增大正则化系数
     * setElasticNetParam正则化范式比(默认0),正则化有两种方式:L1(Lasso)和L2(Ridge),L1用于特征的稀疏化,L2用于防止过拟合
     * setLabelCol设置标签列
     * setFeaturesCol设置特征列
     * setPredictionCol设置预测列
     * setThreshold设置二分类阈值
     */

    val lr = new LogisticRegression()
        .setFeaturesCol("feature")
        .setLabelCol("label")
        .setPredictionCol("predict")
        .setRegParam(0.5)
        .setElasticNetParam(0)
        .setStandardization(true)
    val threhold = 0.5

    val model = lr.fit(train_vec)

    println( s"Model Coefficient \n " +
      s"${model.weights.toArray.map( _.toFloat ).mkString(",")}. \n" +
      s"Model Bias \n " +
      s"${model.intercept}. \n")


    /**
     *  手写 prediction，因为spark的包有bug
     *  通过withColumn记录共享变量，因为broadcast也报错
     */
    val prediction = test_vec.select(
      $"userid",$"feature",$"label",
      lit(model.weights.toArray.mkString(",")).as("weights"),
      lit(model.intercept).as("bias"),
      lit(threhold).as("threhold")
    ).rdd.map( l => {
      val feature = l.getAs[org.apache.spark.mllib.linalg.DenseVector]("feature")
      val weights = l.getString(3).split(",").map( _.toDouble )
      val threhold = l.getAs[Double]("threhold")
      var rawPrediction = l.getAs[Double]("bias")
      for( i <- (0 to weights.size -1 ) ){
        rawPrediction += weights(i)*feature(i)
      }
      val probability = 1/(1 + Math.exp(-rawPrediction) )
      val predict = if (probability > threhold) 1.0 else 0.0
      (l.getString(0), l.getAs[org.apache.spark.mllib.linalg.DenseVector](1), rawPrediction, probability, predict, l.getDouble(2))
    }).toDF("userid","feature","rawPrediction","probability","predict","label")


    /**
     *  评估
     */

    val predictionRdd = prediction.select("predict","label").rdd.map{
      case Row(predict:Double,label:Double)=>(predict,label)
    }
    val metrics = new MulticlassMetrics(predictionRdd)
    val precision = metrics.precision
    val recall = metrics.recall
    val weightedPrecision = metrics.weightedPrecision
    val weightedRecall= metrics.weightedRecall
    val f1 = metrics.weightedFMeasure

    println(s"LR评估结果：\n " +
      s"分类准确率：${precision}\n " +
      s"分类召回率：${recall}\n " +
      s"加权正确率：${weightedPrecision}\n " +
      s"加权召回率：${weightedRecall}\n " +
      s"F1值：${f1}")



    sc.stop()
    println(s"Complete with ${(System.currentTimeMillis() - t1) / 60000} min.")


  }

}
