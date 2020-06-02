package smk.api

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 *  用户基本信息查询 API
 *  1.  -age  指定年龄或年龄范围
 *  2.  -gender 指定性别
 *  3.  -type 指定 target 或 vip 或 both  或 all （全体用户）, 默认 both
 *  4.  -month  指定月份
 *  5.  -path 指定路径，或默认
 */

object ApiBasicInfo {

  def main(args: Array[String]): Unit = {

    var age_cond = "1=1"
    var gender_cond = "1=1"
    var part_type_cond = "1=1"
    var month_cond = "1=1"
    var table = "basic_info_target_m"
    var filepath = "/suyan/chenzhihao/api_output/basic_info_"+ new SimpleDateFormat("HHmmss").format(new Date)

    args.sliding(2,2).toList.collect{
      case Array("-age", age) =>  age_cond += s" and age$age "
      case Array("-gender", gender) =>  gender_cond += s" and gender='${if(gender=='m') '1' else '2'}' "
      case Array("-type", part_type) => if(part_type =="all") table="basic_info_m" else if (part_type!="both") part_type_cond+=s" and part_type='$part_type' "
      case Array("-month", month) =>  month_cond += s" and part_month='$month' "
      case Array("-path", path) => filepath = path
    }

    val conf = new SparkConf()
      .set("spark.executor.instances","2")
      .set("spark.executor.cores","1")
      .set("spark.executor.memory","4G")
    val sc = SparkContext.getOrCreate(conf)
    val ssc: SQLContext = new HiveContext(sc) // 同 spark-shell 中的 sqlContext
    import ssc.implicits._
    ssc.sql("set hive.exec.dynamic.partition=true;")
    ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val res = ssc.sql(
      s"""
         |select * from suyanli.$table
         |where $age_cond and $gender_cond and $part_type_cond and $month_cond
         |""".stripMargin)


    res.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(filepath)

    sc.stop()

  }

}
