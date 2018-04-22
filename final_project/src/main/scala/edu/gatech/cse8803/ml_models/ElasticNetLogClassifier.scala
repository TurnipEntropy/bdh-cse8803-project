
package edu.gatech.cse8803.ml_models

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, BinaryLogisticRegressionSummary}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import edu.gatech.cse8803.main.Main.{PatientTuple, KeyTuple, ValueTuple}

class ElasticNetLogClassifier (elasticNetParam: Double = 0.15, fitIntercept: Boolean = true,
                               maxIter: Int = 10, standardize: Boolean = false,
                               threshold: Double = 0.5, addWeightsCol: Boolean = false,
                               weightsCol: String = "classWeightCol", labelCol: String = "label",
                               featuresCol: String = "features"){
  var model: LogisticRegression = if (addWeightsCol) {
    new LogisticRegression().setMaxIter(maxIter).setElasticNetParam(elasticNetParam).
                             setFitIntercept(fitIntercept).setStandardization(standardize).
                             setThreshold(threshold).setLabelCol(labelCol).
                             setFeaturesCol(featuresCol).setWeightCol(weightsCol)
  } else {
    new LogisticRegression().setMaxIter(maxIter).setElasticNetParam(elasticNetParam).
                             setFitIntercept(fitIntercept).setStandardization(standardize).
                             setThreshold(threshold).setLabelCol(labelCol).setFeaturesCol(featuresCol)
  }

  var sqlContext: SQLContext = _
  private var lrm: LogisticRegressionModel = _
  var prevPredictions: DataFrame = _
  var prevDataFrame: String = "0x0"

  def train(data: DataFrame): LogisticRegressionModel =  {
    //have to turn it into a DataFrame, with the first entry being the label
    //and the rest being the data.
    //k,v = (Long, Long), (Timestamp, Int, SummedGcsPatient)

    lrm = model.fit(data)
    lrm
  }

  def predict(data: DataFrame): DataFrame  = {
    if (data.toString == prevDataFrame) {
      prevPredictions
    } else {
      prevDataFrame = data.toString
      val pred = lrm.transform(data)
      prevPredictions = pred
      pred
    }

  }

  def getEvaluationMetrics(data: DataFrame): RDD[(Int, Int)] = {
    val preds = if (data.toString == prevDataFrame) prevPredictions else predict(data)
    val confusionMatrix = preds.map({
      case row => if (row(0).toString.toDouble == 0.0 && row(4).toString.toDouble == 0.0){
        (0, 1)
      } else if (row(0).toString.toDouble == 0.0 && row(4).toString.toDouble == 1.0) {
        (-1, 1)
      } else if (row(0).toString.toDouble == 1.0 && row(4).toString.toDouble == 1.0) {
        (1, 1)
      } else {
        (-2, 1)
      }
    }).reduceByKey(_+_)
    confusionMatrix
  }

  def getAUC(): Double = {
    getBinarySummary().areaUnderROC
  }

  def getBinarySummary(): BinaryLogisticRegressionSummary = {
    lrm.summary.asInstanceOf[BinaryLogisticRegressionSummary]
  }

  def getModel(): LogisticRegressionModel = {
    lrm
  }

  def convertRDDtoDF(data: RDD[(KeyTuple, ValueTuple)]): DataFrame = {
    this.sqlContext = new SQLContext(data.context)
    val segmented = data.map({
      case (k,v) => (v._2.toDouble, Vectors.dense(v._3.bpDia, v._3.bpSys, v._3.heartRate,
                     v._3.respRate, v._3.temp, v._3.spo2, v._3.gcs, v._3.age.toDouble))
    })
    this.sqlContext.createDataFrame(segmented).toDF("label", "features")
  }
}
