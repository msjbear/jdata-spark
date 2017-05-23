package com.sjmei.jdata.xgboost

/**
  * Created by meishangjian on 2017/5/3.
  */
import com.sjmei.jdata.utils.{AlgoUtils, DataLoadUtils, SubmissionEvalUtils}
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostEstimator, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
import scopt.OptionParser

import scala.collection.mutable

object SparkXgboostWithDataFrame {

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
          case "train_cv" => train_cv(spark, params)
          case "predict" => predict(spark, params)
          case "predict_leaf" => predictLeafs(spark, params)
          case "train_leaf_lr" => trainLeafsWithLR(spark, params)
          case "eval_leaf_lr" => evalLeafsWithLR(spark, params)
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

    val (trainDF, testDF) = DataLoadUtils.loadTrainData(sparkSession, params.inputPath, params.fracTest)
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
    * train xgboost model with cross validation
    *
    * @param sparkSession
    * @param params
    */
  def train_cv(sparkSession: SparkSession, params: Params): Unit = {

    val (trainDF, testDF) = DataLoadUtils.loadTrainData(sparkSession, params.inputPath, params.fracTest)
    // start training
    val paramMap = List(
      "eta" -> 0.05f,
      "max_depth" -> 8,
      "objective" -> "binary:logistic" // ,"eval_metric" -> "logloss"
    ).toMap
    // Set up Pipeline.
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val estimator = new XGBoostEstimator(paramMap)
    // assigning general parameters
    estimator.set(estimator.useExternalMemory, false)
      .set(estimator.round, params.numRound)
      .set(estimator.nWorkers, params.nWorkers)
      .set(estimator.missing, Float.NaN)
      .setFeaturesCol("features")
      .setLabelCol("label")

    // assigning general parameters
    stages += estimator
    val pipeline = new Pipeline().setStages(stages.toArray)
    // Fit the Pipeline.
    val startTime = System.nanoTime()

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    val paramGrid = new ParamGridBuilder()
      .addGrid(estimator.maxDepth, Array(8, 10))
      .addGrid(estimator.eta, Array(0.1, 0.05))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(trainDF)
    cvModel.write.overwrite.save(params.modelPath)
    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val train_predict = cvModel.transform(trainDF).select("label","prediction","probability")
    val test_predict = cvModel.transform(testDF).select("label","prediction","probability")
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val train_accuracy = evaluator.evaluate(train_predict)
    val test_accuracy = evaluator.evaluate(test_predict)
    println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")
    // $example off$

    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    train_predict.printSchema()
    train_predict.select("label","prediction","probability").show(10)

    trainDF.unpersist(blocking = false)
    testDF.unpersist(blocking = false)
  }

  /**
    * predict xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def predict(sparkSession: SparkSession, params: Params): Unit = {

    val datasets = DataLoadUtils.loadPredictDataOrc(sparkSession, params.inputPath)
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
    val predicts = result.select("user_id", "sku_id", "probabilities", "prediction")
      .map(row => {
      (row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[DenseVector].toArray(1),
        row.get(3).asInstanceOf[Double])
    })
    predicts.write.mode(SaveMode.Overwrite).orc(params.resultPath)

    val sqlCommand = s"ALTER TABLE dev.dev_temp_msj_risk_jdata_predict_result ADD IF NOT EXISTS PARTITION(dt='${params.initDate}', result_type='${params.resultType}')"
    sparkSession.sql(sqlCommand)

    val df_submission = result.select("user_id", "sku_id", "probabilities", "prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[DenseVector].toArray(1),
        row.get(3).asInstanceOf[Double])
      }).toDF("user_id", "sku_id", "prob", "predict")

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
    * predict each tree score from xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def predictLeafs(sparkSession: SparkSession, params: Params): Unit = {

    val datasets = DataLoadUtils.loadEvalDataOrc(sparkSession, params.inputPath)
    // start training
    // xgboost-spark appends the column containing prediction results
    val xgboostModel = XGBoostModel.load(params.modelPath)
    val result = xgboostModel.transformLeaf(datasets)
    println("JRDM:XGBoost")
    result.printSchema()
    result.show(100)

    import sparkSession.implicits._
    val predicts = result.select("user_id", "sku_id", "label", "predLeaf")
      .map(row => {
        (row.get(0).asInstanceOf[String] + sep
          + row.get(1).asInstanceOf[String] + sep
          + row.get(2).asInstanceOf[Int] + sep
          + row.get(3).asInstanceOf[scala.collection.mutable.WrappedArray[Float]].mkString(sep))
      })

    val predRDD = result.select("label", "predLeaf").rdd
      .map(row => {
        LabeledPoint(row.get(0).asInstanceOf[Int].toDouble,
          org.apache.spark.mllib.linalg.Vectors.dense(
            row.get(1).asInstanceOf[scala.collection.mutable.WrappedArray[Float]]
              .toArray.map(_.toDouble)))
      })

    predRDD.take(10)
    val libsvmFeatsPath = "sjmei/tests/xgboost/gbdt_libsvm_feats"
    AlgoUtils.deleteFile(libsvmFeatsPath)
    MLUtils.saveAsLibSVMFile(predRDD, libsvmFeatsPath)
    predicts.show(100)
    predicts.write.mode(SaveMode.Overwrite).text(params.resultPath)

    datasets.unpersist(blocking = false)
    predicts.unpersist(blocking = false)
  }

  /**
    * predict each tree score from xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def trainLeafsWithLR(sparkSession: SparkSession, params: Params): Unit = {

    val (trainDF, testDF) = DataLoadUtils.loadTrainData(sparkSession, params.inputPath, params.fracTest)
    // start training
    val paramMap = List(
      "eta" -> 0.05f,
      "max_depth" -> 5,
      "objective" -> "binary:logistic",
      "eval_metric" -> "logloss"
    ).toMap
    val xgboostModel = XGBoost.trainWithDataFrame(
      trainDF, paramMap, params.numRound, params.nWorkers, useExternalMemory = true)
    // xgboost-spark appends the column containing prediction results
    val result = xgboostModel.transformLeaf(trainDF)

    AlgoUtils.deleteFile(params.modelPath+".LeafsWithLR.xgb")
    xgboostModel.save(params.modelPath+".LeafsWithLR.xgb")

    trainDF.unpersist(blocking = false)
    testDF.unpersist(blocking = false)

    println("JRDM:XGBoost")
    result.printSchema()
    result.show(100)

    import sparkSession.implicits._
    val df_gbdt_result = result.select("user_id", "sku_id", "label", "predLeaf")
      .map(row => {
        (row.get(0).asInstanceOf[String],
          row.get(1).asInstanceOf[String],
          row.get(2).asInstanceOf[Int],
        Vectors.dense(row.get(3).asInstanceOf[scala.collection.mutable.WrappedArray[Float]].toArray.map(_.toDouble)))
      }).toDF("user_id", "sku_id", "label", "features")

    val df_vecIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("features_vec")
      .setMaxCategories(200)
      .fit(df_gbdt_result)
      .transform(df_gbdt_result)

    val dataframes = df_vecIndexer.randomSplit(Array(0.9, 0.1), seed = 12345)

    val training = dataframes(0).cache()
    val testing = dataframes(1).cache()

    val lr = new LogisticRegression()
      .setFeaturesCol("features_vec")
      .setLabelCol("label")
      .setRegParam(0.0)
      .setElasticNetParam(0.0)
      .setMaxIter(100)
      .setTol(1E-6)
      .setFitIntercept(true)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.0, 0.1))
      .addGrid(lr.maxIter, Array(100, 50))
      .build()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice

    val cvModel = cv.fit(training)
    cvModel.write.overwrite.save(params.modelPath+".LeafsWithLR.lr")

    // Shows the best parameters
    cvModel.bestModel match {
      case pipeline: Pipeline =>
        pipeline.getStages.zipWithIndex.foreach { case (stage, index) =>
          println(s"Stage[${index + 1}]: ${stage.getClass.getSimpleName}")
          println(stage.extractParamMap())
        }
    }

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val train_predict = cvModel.transform(training).select("label","prediction","probability")
    val test_predict = cvModel.transform(testing).select("label","prediction","probability")
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val train_accuracy = evaluator.evaluate(train_predict)
    val test_accuracy = evaluator.evaluate(test_predict)
    println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")

    result.unpersist(blocking = false)
    training.unpersist(blocking = false)
    testing.unpersist(blocking = false)
  }

  def evalLeafsWithLR(sparkSession: SparkSession, params: Params): Unit = {

    val datasets = DataLoadUtils.loadEvalDataOrc(sparkSession, params.inputPath)
    // start training
    // xgboost-spark appends the column containing prediction results
    val xgboostModel = XGBoostModel.load(params.modelPath+".LeafsWithLR.xgb")
    val xg_result = xgboostModel.transformLeaf(datasets)
    println("JRDM:XGBoost")
    xg_result.printSchema()
    xg_result.show(10)

    import sparkSession.implicits._
    val df_gbdt_result = xg_result.select("user_id", "sku_id", "label", "predLeaf")
      .map(row => {
        (row.get(0).asInstanceOf[String],
          row.get(1).asInstanceOf[String],
          row.get(2).asInstanceOf[Int],
          Vectors.dense(row.get(3).asInstanceOf[scala.collection.mutable.WrappedArray[Float]].toArray.map(_.toDouble)))
      }).toDF("user_id", "sku_id", "label", "features")

    val df_vecIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("features_vec")
      .setMaxCategories(1000)
      .fit(df_gbdt_result)
      .transform(df_gbdt_result)

    val cvModel = CrossValidatorModel.load(params.modelPath+".LeafsWithLR.lr")
    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val lr_result = cvModel.transform(df_vecIndexer).select("user_id", "sku_id", "label","probability","prediction")
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

    val script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)
    val submission_result = sparkSession.sql(script_sql)
    println("JRDM: submission cnt: " + submission_result.count())

    lr_result.cache()
    submission_result.cache()

    SubmissionEvalUtils.jdata_report(predicts, submission_result)

    datasets.unpersist(blocking = false)
    xg_result.unpersist(blocking = false)
    lr_result.unpersist(blocking = false)
    df_gbdt_result.unpersist(blocking = false)
  }

  /**
    * predict and evaluate xgboost model
    *
    * @param sparkSession
    * @param params
    */
  def predict_eval(sparkSession: SparkSession, params: Params): Unit = {

    val datasets = DataLoadUtils.loadEvalDataOrc(sparkSession, params.inputPath)
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
    val predicts = result.select("user_id", "sku_id", "label", "probabilities", "prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[Int],
        row.get(3).asInstanceOf[DenseVector].toArray(1),
        row.get(4).asInstanceOf[Double])
    }).toDF("user_id", "sku_id", "label", "prob", "predict")
    predicts.write.mode(SaveMode.Overwrite).orc(params.resultPath)

    val sqlCommand = s"ALTER TABLE dev.dev_temp_msj_risk_jdata_eval_result ADD IF NOT EXISTS PARTITION(dt='${params.initDate}',result_type='${params.resultType}')"
    sparkSession.sql(sqlCommand)

    predicts.createOrReplaceTempView("future_predict_table")

    val script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)

    val submission_result = sparkSession.sql(script_sql)
    submission_result.map(_.mkString(sep)).write.mode(SaveMode.Overwrite).text(params.resultPath+".submit")
    println("JRDM: submission cnt: " + submission_result.count())
    predicts.cache()
    submission_result.cache()

    SubmissionEvalUtils.jdata_report(predicts, submission_result)

    datasets.unpersist(blocking = false)
    predicts.unpersist(blocking = false)
    submission_result.unpersist(blocking = false)

  }
}
