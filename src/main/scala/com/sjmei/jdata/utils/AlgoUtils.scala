package com.sjmei.jdata.utils

import java.io.{FileInputStream, IOException, StringWriter}
import java.util.Properties

import com.sjmei.jdata.sparkml.RandomForestTask.Params
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql._
import org.apache.spark.utils.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine


/**
  * Created by meishangjian on 2017/4/26.
  */
object AlgoUtils extends Logging {

  val props: Properties = getGraphProperty()
  val FIELD_SEP = "\u0001"
//  val FIELD_SEP = "\t"
  val NUM_PARTITIONS = props.getProperty("GRAPH.REPARTITIONS.NUM").toInt

  def getSparkContext(clazzName: String): SparkContext ={
    val conf = new SparkConf().setAppName(clazzName)
    val os = System.getProperty("os.name");
    if(os.toLowerCase().startsWith("windows")){
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)

    sc
  }

  /**
    * init sparksession
    * @param appName
    * @return
    */
  def getSparkSession(appName: String): SparkSession = {
    val builder = SparkSession.builder
    val os = System.getProperty("os.name");
    if(os.toLowerCase().startsWith("windows")){
      builder.master("local")
    }

    val spark = builder.appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport
      .getOrCreate()

    spark
  }

  def getSparkSession(conf: SparkConf): SparkSession = {
    val builder = SparkSession.builder
    val os = System.getProperty("os.name");
    if (os.toLowerCase().startsWith("windows")) {
      builder.master("local")
    }

    val spark = builder.config(conf)
      .enableHiveSupport
      .getOrCreate()

    spark
  }

  private def initPropertyFile(filePath: String): Properties = {
    val properties = new Properties()
    try {
      properties.load(new FileInputStream(filePath))
    } catch {
      case e: IOException => {
        logError("JRDM:" + "Failed to open configuration file")
        logError("JRDM:" + e.getStackTrace+e.getMessage)
      }
    }
    properties
  }

  def getGraphProperty(): Properties = {
    val filePath = this.getClass().getClassLoader().getResource("graph-jdata.properties").getFile()
    val props = initPropertyFile(filePath)
    if(props == null) {
      logError("JRDM:" + "Props is null! Plase initialize it first!")
    }

    return props
  }

  def saveRFPredictResult(spark:SparkSession, predicts: DataFrame, params: Params): Unit = {

    import spark.implicits._
    predicts.printSchema()
    predicts.take(10).foreach(println)

    val result = predicts.select("user_id","sku_id","probability","prediction").map(row =>
      (row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[DenseVector].toArray(1),
        row.get(3).asInstanceOf[Double])
    )

    result.show(10)
    result.coalesce(20).write.mode(SaveMode.Overwrite).orc(params.output)

    val sqlCommand = s"ALTER TABLE dev.dev_temp_msj_risk_jdata_predict_result ADD IF NOT EXISTS PARTITION(dt='${params.initDate}', result_type='${params.resultType}')"
    spark.sql(sqlCommand)
  }

  def saveRFEvalResult(spark:SparkSession, predicts: DataFrame, params: Params): Unit = {

    import spark.implicits._
    predicts.printSchema()
    predicts.take(10).foreach(println)

    val result = predicts.select("user_id","sku_id","label","probability","prediction").map(row =>
      (row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[Int],
        row.get(3).asInstanceOf[DenseVector].toArray(1),
        row.get(4).asInstanceOf[Double])
    )

    result.show(10)
    result.coalesce(20).write.mode(SaveMode.Overwrite).orc(params.output)

    val sqlCommand = s"ALTER TABLE dev.dev_temp_msj_risk_jdata_eval_result ADD IF NOT EXISTS PARTITION(dt='${params.initDate}', result_type='${params.resultType}')"
    spark.sql(sqlCommand)
  }

  /**
    * normalize the probability
    *
    * @param spark
    * @param predicts
    * @return
    */
  def saveNormProbResult(spark:SparkSession, predicts: DataFrame, output:String): Unit = {

    import spark.implicits._
    predicts.printSchema()
    predicts.take(10).foreach(println)
    /**
      * root
      *|-- metas: string (nullable = true)
      *|-- label: integer (nullable = true)
      *|-- features: vector (nullable = true)
      *|-- rawPrediction: vector (nullable = true)
      *|-- probability: vector (nullable = true)
      *|-- prediction: double (nullable = true)
      *
      */
    val probability = predicts.select("user_id","sku_id","probability","prediction").rdd.map(row => {
      val prediction = row.get(2).asInstanceOf[DenseVector]
      val prob: Double = prediction.values(1)
      if (prob > 0.0d) {
        (0.0d, prob)
      } else {
        (prob.abs, 0.0d)
      }
    }).cache()

    val maxNeg = probability.map(_._1).max
    val maxPos = probability.map(_._2).max
    logWarning("JRDM:maxpos:" + maxPos + ",maxneg:" + maxNeg)
    //normalize
    val result = predicts.select("user_id","sku_id","probability","prediction").map(row => {
      val prediction = row.get(2).asInstanceOf[DenseVector]
      var prob: Double = prediction.values(1)
      var newprob = (0.0d, 0.0d)
      if (prob > 0.0d) {
        newprob = (0.5d - prob / maxPos * 0.5, prob / maxPos * 0.5 + 0.5)
      } else {
        prob = prob.abs
        newprob = (prob / maxNeg * 0.5 + 0.5, 0.5d - prob / maxNeg * 0.5)
      }

      (row.get(0).asInstanceOf[String]+ FIELD_SEP
        + row.get(1).asInstanceOf[String] + FIELD_SEP
        + newprob._1 + FIELD_SEP + newprob._2 + FIELD_SEP
        + row.get(3).asInstanceOf[Double])
    })

    probability.unpersist(blocking = false)
    result.take(10).foreach(println)

    result.coalesce(50).write.mode(SaveMode.Overwrite).text(output)
  }


  def saveEvalNormProbResult(spark:SparkSession, predicts: DataFrame, output:String): Unit = {

    import spark.implicits._
    predicts.printSchema()
    predicts.take(10).foreach(println)
    /**
      * root
      *|-- metas: string (nullable = true)
      *|-- label: integer (nullable = true)
      *|-- features: vector (nullable = true)
      *|-- rawPrediction: vector (nullable = true)
      *|-- probability: vector (nullable = true)
      *|-- prediction: double (nullable = true)
      *
      */
    val probability = predicts.select("user_id","sku_id","label","probability","prediction").rdd.map(row => {
      val prediction = row.get(3).asInstanceOf[DenseVector]
      val prob: Double = prediction.values(1)
      if (prob > 0.0d) {
        (0.0d, prob)
      } else {
        (prob.abs, 0.0d)
      }
    }).cache()

    val maxNeg = probability.map(_._1).max
    val maxPos = probability.map(_._2).max
    logWarning("JRDM:maxpos:" + maxPos + ",maxneg:" + maxNeg)
    //normalize
    val result = predicts.select("user_id","sku_id","label","probability","prediction").map(row => {
      val prediction = row.get(3).asInstanceOf[DenseVector]
      var prob: Double = prediction.values(1)
      var newprob = (0.0d, 0.0d)
      if (prob > 0.0d) {
        newprob = (0.5d - prob / maxPos * 0.5, prob / maxPos * 0.5 + 0.5)
      } else {
        prob = prob.abs
        newprob = (prob / maxNeg * 0.5 + 0.5, 0.5d - prob / maxNeg * 0.5)
      }

      (row.get(0).asInstanceOf[String]+ FIELD_SEP
        + row.get(1).asInstanceOf[String] + FIELD_SEP
        + row.get(2).asInstanceOf[Int] + FIELD_SEP
        + newprob._1 + FIELD_SEP + newprob._2 + FIELD_SEP
        + row.get(3).asInstanceOf[Double])
    })

    probability.unpersist(blocking = false)
    result.take(10).foreach(println)

    result.coalesce(50).write.mode(SaveMode.Overwrite).text(output)
  }

  def deleteFile(fileStr: String): Unit = {
    val conf: Configuration = new Configuration();
    val fs: FileSystem = FileSystem.get(conf);
    val fileName: Path = new Path(fileStr)
    if (fs.exists(fileName)) {
      fs.delete(fileName, true)
    }
  }

  def genSubmissionResultSql(templatePath: String, init_date: String): String = {

    val velocityEngine = new VelocityEngine();
    val context = new VelocityContext();
    context.put("init_date", init_date);

    val sw = new StringWriter();
    velocityEngine.mergeTemplate(templatePath, "utf-8", context, sw);
    println(sw.toString())

    sw.toString
  }

  def genModelBlendFeatureSql(templatePath: String): String = {

    val velocityEngine = new VelocityEngine();
    val context = new VelocityContext();

    val sw = new StringWriter();
    velocityEngine.mergeTemplate(templatePath, "utf-8", context, sw);
    println(sw.toString())

    sw.toString
  }

}

