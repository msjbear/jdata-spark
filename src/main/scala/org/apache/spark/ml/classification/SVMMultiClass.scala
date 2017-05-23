package org.apache.spark.ml.classification

import breeze.linalg.DenseVector
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Created by cdmeishangjian on 2016/2/2.
 */

class SVMMultiClassOVAModel(classModels: Array[SVMModel]) extends org.apache.spark.mllib.classification.ClassificationModel with Serializable {

  val classModelsWithIndex = classModels.zipWithIndex

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return an RDD[Double] where each entry contains the corresponding prediction
   */
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val localClassModelsWithIndex = classModelsWithIndex
    val bcClassModels = testData.context.broadcast(localClassModelsWithIndex)
    testData.mapPartitions { iter =>
      val w = bcClassModels.value
      iter.map(v => predictPoint(v, w))
    }
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return predicted category from the trained model
   */
  override def predict(testData: Vector): Double = predictPoint(testData, classModelsWithIndex)

  def predictPoint(testData: Vector, models: Array[(SVMModel, Int)]): Double =
    models
      .map { case (classModel, classNumber) => (classModel.predict(testData), classNumber)}
      .maxBy { case (score, classNumber) => score}
      ._2

}


object SVMMultiClassOVAWithSGD {

  /**
   * Train a Multiclass SVM model given an RDD of (label, features) pairs,
   * using One-vs-Rest method - create one SVMModel per class with SVMWithSGD.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double): SVMMultiClassOVAModel = {

    val numClasses = input.map(_.label).max().toInt

    val classModels = (0 until numClasses).map { classId =>

      val inputProjection = input.map { case LabeledPoint(label, features) =>
        LabeledPoint(if (label == classId) 1.0 else 0.0, features)}.cache()
      val model = SVMWithSGD.train(inputProjection, numIterations, stepSize, regParam, miniBatchFraction)
      inputProjection.unpersist(false)

      model.clearThreshold()
      model

    }.toArray

    new SVMMultiClassOVAModel(classModels)

  }

  /**
   * Train a Multiclass SVM model given an RDD of (label, features) pairs,
   * using One-vs-Rest method - create one SVMModel per class with SVMWithSGD.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(input: RDD[LabeledPoint], numIterations: Int, stepSize: Double, regParam: Double): SVMMultiClassOVAModel =
    train(input, numIterations, stepSize, regParam, 1.0)

  /**
   * Train a Multiclass SVM model given an RDD of (label, features) pairs,
   * using One-vs-Rest method - create one SVMModel per class with SVMWithSGD.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(input: RDD[LabeledPoint], numIterations: Int): SVMMultiClassOVAModel = train(input, numIterations, 1.0, 0.01, 1.0)

  def main (args: Array[String]){

    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(sparkConf)

    val digits_train = sc.textFile("jrdm-dm/data/mllib/multiclass/pendigits.tra").map(line => DenseVector(line.split(",").map(_.trim().toDouble))).map( v => LabeledPoint(v(-1),Vectors.dense(v(0 to 15).toArray))).cache()
    val digits_test = sc.textFile("jrdm-dm/data/mllib/multiclass/pendigits.tes").map(line => DenseVector(line.split(",").map(_.trim().toDouble))).map( v => LabeledPoint(v(-1),Vectors.dense(v(0 to 15).toArray)))


    val model = SVMMultiClassOVAWithSGD.train(digits_train, 100)
    val predictionAndLabel = digits_test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / digits_test.count()

    val scoreAndLabels = digits_test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    scoreAndLabels.take(5)

  }

}