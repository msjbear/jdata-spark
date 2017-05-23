package com.sjmei.jdata.xgboost

/**
  * Created by meishangjian on 2017/5/10.
  */
import com.sjmei.jdata.utils.{AlgoUtils, DataLoadUtils, SubmissionEvalUtils, VelocityUtils}
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql._
import scopt.OptionParser

object SparkModelStackingMain {

  val sep = AlgoUtils.FIELD_SEP
  val numPartitions = AlgoUtils.NUM_PARTITIONS

  case class Params(
                     inputPath: String = null,
                     modelPath: String = null,
                     resultPath: String = null,
                     taskType: String = null,
                     initDate: String = "2016-04-06",
                     resultType: String = "xgb_predict_eval",
                     nWorkers: Int = 20,
                     numRound: Int = 200,
                     isCvModel: Boolean = false,
                     fracTest: Double = 0.2) extends AbstractParams[Params]

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
          case "train_xgboost" => train_xgboost(spark, params)
          case "train_xgboost_stacking" => train_xgboost_stacking(spark, params)
          case "predict" => predict(spark, params)
          case "predict_eval" => predict_eval(spark, params)
          case "train_xgboost_stacking" => train_xgboost_stacking(spark, params)
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
  def train_xgboost(sparkSession: SparkSession, params: Params): Unit = {

    var startDate = "2016-03-31"
    var count = 3
    while(startDate < params.initDate && count <= 6){
      val (trainDF, testDF) = DataLoadUtils.loadTrainData(sparkSession, params.inputPath+s"_part$count", params.fracTest)
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

      AlgoUtils.deleteFile(params.modelPath +s"_part$count")
      xgboostModel.save(params.modelPath +s"_part$count")

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
      val train_accuracy = evaluator.evaluate(predTrainResult)
      val test_accuracy = evaluator.evaluate(predTestResult)
      println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")

      trainDF.unpersist(blocking = false)
      testDF.unpersist(blocking = false)
      predTestResult.unpersist(blocking = false)

      startDate = VelocityUtils.plusOneDay(startDate)
      count +=1
    }

  }

  /**
    * predict xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def predict(sparkSession: SparkSession, params: Params): Unit = {

    var startDate = "2016-03-31"
    var count = 1
    val datasets = DataLoadUtils.loadPredictDataOrc(sparkSession, params.inputPath)
    while(startDate < params.initDate && count <= 6){
      // start training
      // xgboost-spark appends the column containing prediction results

      val xgboostModel = XGBoostModel.load(params.modelPath +s"_part$count")
      val result = xgboostModel.transform(datasets)

      import sparkSession.implicits._
      val predicts = result.select("user_id", "sku_id", "probabilities", "prediction")
        .map(row => {(row.get(0).asInstanceOf[String],
          row.get(1).asInstanceOf[String],
          row.get(2).asInstanceOf[DenseVector].toArray(1),
          row.get(3).asInstanceOf[Double])
        }).toDF("user_id", "sku_id", "prob", "predict")
      predicts.createOrReplaceTempView("predict_result_table"+s"_part$count")
      predicts.show(10)

      startDate = VelocityUtils.plusOneDay(startDate)
      count +=1

    }

    val script_sql = AlgoUtils.genModelBlendFeatureSql("model_blend_pred_feature.sql")
    val df_blend_feature = sparkSession.sql(script_sql)

    val dataSets = new VectorAssembler()
      .setInputCols(Array("prob_1","prob_2","prob_3","prob_4","prob_5","prob_6"))
      .setOutputCol("lr_features").transform(df_blend_feature)

    val cvModel = CrossValidatorModel.load(params.modelPath+".lr")


    import sparkSession.implicits._
    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val df_predict = cvModel.transform(dataSets)
      .select("user_id", "sku_id","probability","prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[DenseVector].toArray(1),
        row.get(3).asInstanceOf[Double])
      }).toDF("user_id", "sku_id", "prob", "predict")
    println("LR_predict:")
    df_predict.show(10)
    df_predict.createOrReplaceTempView("future_predict_table")

    val sub_script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)

    val submission_result = sparkSession.sql(sub_script_sql)
    submission_result.map(_.mkString(sep)).write.mode(SaveMode.Overwrite).text(params.resultPath + ".submit")
    println("JRDM: submission cnt: " + submission_result.count())

    datasets.unpersist(blocking = false)

  }

  /**
    * predict and evaluate xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def train_xgboost_stacking(sparkSession: SparkSession, params: Params): Unit = {

    var startDate = "2016-03-31"
    var count = 1
    val datasets = DataLoadUtils.loadEvalData(sparkSession, params.inputPath)
    while(startDate < params.initDate  && count <= 6){
      // start training
      // xgboost-spark appends the column containing prediction results

      val xgboostModel = XGBoostModel.load(params.modelPath +s"_part$count")
      val result = xgboostModel.transform(datasets)

      import sparkSession.implicits._
      val predicts = result.select("user_id", "sku_id", "label", "probabilities", "prediction")
        .map(row => {(row.get(0).asInstanceOf[String],
          row.get(1).asInstanceOf[String],
          row.get(2).asInstanceOf[Int],
          row.get(3).asInstanceOf[DenseVector].toArray(1),
          row.get(4).asInstanceOf[Double])
        }).toDF("user_id", "sku_id", "label", "prob", "predict")

      predicts.createOrReplaceTempView("future_predict_table")

      val script_sql2 = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)
      val submission_result = sparkSession.sql(script_sql2)
      println("JRDM: submission cnt: " + submission_result.count())

      submission_result.cache()
      SubmissionEvalUtils.jdata_report(predicts, submission_result)

      submission_result.unpersist(blocking = false)

      predicts.createOrReplaceTempView("predict_eval_result_table"+s"_part$count")
      predicts.show(10)

      startDate = VelocityUtils.plusOneDay(startDate)
      count +=1

    }

    val script_sql = AlgoUtils.genModelBlendFeatureSql("model_blend_feature.sql")
    val df_blend_feature = sparkSession.sql(script_sql)
    println("JRDM: df_blend_feature")
    val dataSets = new VectorAssembler()
      .setInputCols(Array("prob_1", "prob_2", "prob_3", "prob_4", "prob_5", "prob_6"))
      .setOutputCol("lr_features").transform(df_blend_feature)

    val dataframes = dataSets.randomSplit(Array(0.9, 0.1), seed = 12345)
    println("JRDM: dataSets")
    val training = dataframes(0).cache()
    val testing = dataframes(1).cache()

    val lr = new LogisticRegression()
      .setFeaturesCol("lr_features")
      .setLabelCol("label")
      .setRegParam(0.01)
      .setElasticNetParam(0.01)
      .setMaxIter(100)
      .setTol(1E-6)
      .setFitIntercept(true)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.0, 0.05))
      .addGrid(lr.maxIter, Array(50, 100))
      .build()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice

    val cvModel = cv.fit(training)
    cvModel.write.overwrite.save(params.modelPath+".lr")

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val train_predict = cvModel.transform(training).select("label","prediction","probability")
    val test_predict = cvModel.transform(testing).select("label","prediction","probability")
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val train_accuracy = evaluator.evaluate(train_predict)
    val test_accuracy = evaluator.evaluate(test_predict)
    println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")

    datasets.unpersist(blocking = false)
    training.unpersist(blocking = false)
    testing.unpersist(blocking = false)

  }


  def predict_eval(sparkSession: SparkSession, params: Params): Unit = {

    var startDate = "2016-03-31"
    var count = 1
    val datasets = DataLoadUtils.loadEvalData(sparkSession, params.inputPath)
    while(startDate < params.initDate  && count <= 6){
      // start training
      // xgboost-spark appends the column containing prediction results

      val xgboostModel = XGBoostModel.load(params.modelPath +s"_part$count")
      val result = xgboostModel.transform(datasets)

      import sparkSession.implicits._
      val predicts = result.select("user_id", "sku_id", "label", "probabilities", "prediction")
        .map(row => {(row.get(0).asInstanceOf[String],
          row.get(1).asInstanceOf[String],
          row.get(2).asInstanceOf[Int],
          row.get(3).asInstanceOf[DenseVector].toArray(1),
          row.get(4).asInstanceOf[Double])
        }).toDF("user_id", "sku_id", "label", "prob", "predict")
      predicts.createOrReplaceTempView("predict_eval_result_table"+s"_part$count")
      predicts.show(10)

      startDate = VelocityUtils.plusOneDay(startDate)
      count +=1

    }

    val script_sql = AlgoUtils.genModelBlendFeatureSql("model_blend_feature.sql")
    val df_blend_feature = sparkSession.sql(script_sql)

    val dataSets = new VectorAssembler()
      .setInputCols(Array("prob_1", "prob_2", "prob_3","prob_4","prob_5","prob_6"))
      .setOutputCol("lr_features").transform(df_blend_feature)

    val cvModel = CrossValidatorModel.load(params.modelPath+".lr")

    val lr_result = cvModel.transform(dataSets).select("user_id", "sku_id", "label","probability","prediction")
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val train_accuracy = evaluator.evaluate(lr_result)
    println(s"Train Accuracy = $train_accuracy")

    import sparkSession.implicits._
    val predicts = lr_result
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[Int],
        row.get(3).asInstanceOf[DenseVector].toArray(1),
        row.get(4).asInstanceOf[Double])
      }).toDF("user_id", "sku_id", "label", "prob", "predict")

    predicts.createOrReplaceTempView("future_predict_table")

    val script_sql2 = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)
    val submission_result = sparkSession.sql(script_sql2)
    println("JRDM: submission cnt: " + submission_result.count())

    lr_result.cache()
    submission_result.cache()

    SubmissionEvalUtils.jdata_report(predicts, submission_result)

    datasets.unpersist(blocking = false)
    lr_result.unpersist(blocking = false)

  }
}
