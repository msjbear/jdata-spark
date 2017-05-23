package com.sjmei.jdata.xgboost

/**
  * Created by meishangjian on 2017/5/3.
  */
import com.sjmei.jdata.utils.{AlgoUtils, DataLoadUtils, SubmissionEvalUtils}
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql._
import scopt.OptionParser

object SparkXgboostUserModel {

  val sep = AlgoUtils.FIELD_SEP
  val numPartitions = AlgoUtils.NUM_PARTITIONS

  case class Params(
                     inputPath: String = null,
                     modelPath: String = null,
                     resultPath: String = null,
                     taskType: String = null,
                     initDate: String = "2016-04-06",
                     dataFormat: String = "orc",
                     resultType: String = "xgb_predict_eval",
                     nWorkers: Int = 20,
                     numRound: Int = 100,
                     isCvModel: Boolean = false,
                     fracTest: Double = 0.1) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()
    // create SparkSession
    val sparkConf = new SparkConf().setAppName("Trainmodel-X")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Booster]))
    val spark = AlgoUtils.getSparkSession(sparkConf)

    val parser = new OptionParser[Params]("Trainmodel-X") {
      head("Trainmodel with Xgboost")
      opt[String]("initDate")
        .text("initDate of predict")
        .action((x, c) => c.copy(initDate = x))
      opt[String]("resultType")
        .text(s"algorithm (classification, regression), default: ${defaultParams.resultType}")
        .action((x, c) => c.copy(resultType = x))
      opt[Int]("nWorkers")
        .text(s"num of workers, default: ${defaultParams.nWorkers}")
        .action((x, c) => c.copy(nWorkers = x))
      opt[Int]("numRound")
        .text(s"number of round(iteration), default: ${defaultParams.numRound}")
        .action((x, c) => c.copy(numRound = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing. If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("dataFormat")
        .text("data format: orc (default)")
        .action((x, c) => c.copy(dataFormat = x))
      opt[Boolean]("isCvModel")
        .text("is cvmodel flag: false (default)")
        .action((x, c) => c.copy(isCvModel = x))
      arg[String]("<inputPath>")
        .text("inputPath to train or predict datasets")
        .required()
        .action((x, c) => c.copy(inputPath = x))
      arg[String]("<modelPath>")
        .text("modelPath path to labeled examples")
        .required()
        .action((x, c) => c.copy(modelPath = x))
      arg[String]("<resultPath>")
        .text("resultPath to labeled examples")
        .required()
        .action((x, c) => c.copy(resultPath = x))
      arg[String]("<taskType>")
        .text("train or predict the rf model")
        .required()
        .action((x, c) => c.copy(taskType = x))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => {
        params.taskType.trim.toLowerCase match {
          case "train" => train(spark, params)
          case "predict" => predict(spark, params)
          case "predict_eval" => predict_eval(spark, params)
          case _ => println("XGBoost method error...")
        }
      }
      case _ => sys.exit(1)
    }

    spark.stop()
  }

  /**
    * train xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def train(sparkSession: SparkSession, params: Params): Unit = {

    val (trainDF, testDF) = DataLoadUtils.loadUserTrainData(sparkSession, params.inputPath, params.fracTest)
    // start training
    val paramMap = List(
      "eta" -> 0.05f,
      "max_depth" -> 8,
      "objective" -> "binary:logistic" // ,"eval_metric" -> "logloss"
    ).toMap
    val xgboostModel = XGBoost.trainWithDataFrame(
      trainDF, paramMap, params.numRound, params.nWorkers, useExternalMemory = true)
    // xgboost-spark appends the column containing prediction results
    val predTestResult = xgboostModel.transform(testDF)
    val predTrainResult = xgboostModel.transform(trainDF)

    predTestResult.show()

    AlgoUtils.deleteFile(params.modelPath)
    xgboostModel.save(params.modelPath)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val train_accuracy = evaluator.evaluate(predTrainResult)
    val test_accuracy = evaluator.evaluate(predTestResult)
    println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")

    import sparkSession.implicits._
    predTestResult.union(predTrainResult).map(_.mkString(sep))
      .write.mode(SaveMode.Overwrite).text(params.resultPath)

    trainDF.unpersist(blocking = false)
    testDF.unpersist(blocking = false)
    predTestResult.unpersist(blocking = false)
  }

  /**
    * predict xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def predict(sparkSession: SparkSession, params: Params): Unit = {

    val datasets = DataLoadUtils.loadUserPredDataOrc(sparkSession, params.inputPath)
    // start training
    // xgboost-spark appends the column containing prediction results
    var result: DataFrame = null
    if(params.isCvModel){
      val xgboostModel = CrossValidatorModel.load(params.modelPath)
      result = xgboostModel.transform(datasets)

    }else{
      val xgboostModel = XGBoostModel.load(params.modelPath)
      result = xgboostModel.transform(datasets)
    }

    println("JRDM:XGBoost")
    result.printSchema()
    result.show(100)

    import sparkSession.implicits._
    val predicts = result.select("user_id","probabilities", "prediction")
      .map(row => {
      (row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[DenseVector].toArray(1),
        row.get(2).asInstanceOf[Double])
    })
    predicts.write.mode(SaveMode.Overwrite).orc(params.resultPath)

    val df_submission = result.select("user_id", "probabilities", "prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[DenseVector].toArray(1),
        row.get(2).asInstanceOf[Double])
      }).toDF("user_id", "prob", "predict")

    df_submission.createOrReplaceTempView("future_predict_table")

    val script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)

    val submission_result = sparkSession.sql(script_sql)
    submission_result.map(_.mkString(sep)).write.mode(SaveMode.Overwrite).text(params.resultPath + ".submit")
    println("JRDM: submission cnt: " + submission_result.count())

    datasets.unpersist(blocking = false)
    predicts.unpersist(blocking = false)
    df_submission.unpersist(blocking = false)
    submission_result.unpersist(blocking = false)
  }

  /**
    * predict and evaluate xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def predict_eval(sparkSession: SparkSession, params: Params): Unit = {

    val datasets = DataLoadUtils.loadUserEvalData(sparkSession, params.inputPath)
    // start training
    // xgboost-spark appends the column containing prediction results
    var result: DataFrame = null
    if(params.isCvModel){
      val xgboostModel = CrossValidatorModel.load(params.modelPath)
      result = xgboostModel.transform(datasets)

    }else{
      val xgboostModel = XGBoostModel.load(params.modelPath)
      result = xgboostModel.transform(datasets)
    }
    println("JRDM:XGBoost")
    result.printSchema()
    result.show(100)

    import sparkSession.implicits._
    val predicts = result.select("user_id", "label", "probabilities", "prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[Int],
        row.get(2).asInstanceOf[DenseVector].toArray(1),
        row.get(3).asInstanceOf[Double])
    }).toDF("user_id", "label", "prob", "predict")
    predicts.write.mode(SaveMode.Overwrite).orc(params.resultPath)

    predicts.createOrReplaceTempView("future_predict_table")

    val script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)

    val submission_result = sparkSession.sql(script_sql)
    submission_result.map(_.mkString(sep)).write.mode(SaveMode.Overwrite).text(params.resultPath+".submit")
    println("JRDM: submission cnt: " + submission_result.count())
    predicts.cache()
    submission_result.cache()

    SubmissionEvalUtils.jdata_user_report(predicts, submission_result)

    datasets.unpersist(blocking = false)
    predicts.unpersist(blocking = false)
    submission_result.unpersist(blocking = false)

  }
}
