import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, BinaryLogisticRegressionSummary}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import edu.gatech.cse8803.main.Main.{PatientTuple, KeyTuple, ValueTuple}

class ElasticNetLogClassifier (elasticNetParam: Double = 0.15, fitIntercept: Boolean = true,
                               maxIter: Int = 10, standardize: Boolean = false,
                               threshold: Double = 0.1){
  var model: LogisticRegression = new LogisticRegression().setMaxIter(maxIter).
                                                           setElasticNetParam(elasticNetParam).
                                                           setFitIntercept(fitIntercept).
                                                           setStandardization(standardize).
                                                           setThreshold(threshold)

  var sqlContext: SQLContext = _
  private var lrm: LogisticRegressionModel = _
  def train(data: RDD[(KeyTuple, ValueTuple)]): LogisticRegressionModel =  {
    //have to turn it into a DataFrame, with the first entry being the label
    //and the rest being the data.
    //k,v = (Long, Long), (Timestamp, Int, SummedGcsPatient)
    val training = convertRDDtoDF(data)

    lrm = model.fit(training)
    lrm
  }

  def predict(data: RDD[(KeyTuple, ValueTuple)]): DataFrame  = {
    val df = convertRDDtoDF(data)
    lrm.transform(df)
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
  def convertRDDtoDF(data: RDD[(KeyTuple, ValueTuple)]) = {
    this.sqlContext = new SQLContext(data.context)
    val segmented = data.map({
      case (k,v) => (v._2.toDouble, Vectors.dense(v._3.bpDia, v._3.bpSys, v._3.heartRate,
                     v._3.respRate, v._3.temp, v._3.spo2, v._3.gcs, v._3.age.toDouble))
    })
    this.sqlContext.createDataFrame(segmented).toDF("label", "features")
  }
}
