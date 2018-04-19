import edu.gatech.cse8803.main.Main.{KeyTuple, ValueTuple}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

class SVM {


  var sqlContext: SQLContext = _
  // def train(data: RDD[(KeyTuple, ValueTuple)]): SVMModel = {
  //   val training = convertRDDtoDF(data)
  //   SVMWithSGD.train(training)
  // }


  def convertRDDtoDF(data: RDD[(KeyTuple, ValueTuple)]) = {
    this.sqlContext = new SQLContext(data.context)
    val segmented = data.map({
      case (k,v) => (v._2.toDouble, Vectors.dense(v._3.bpDia, v._3.bpSys, v._3.heartRate,
                     v._3.respRate, v._3.temp, v._3.spo2, v._3.gcs, v._3.age.toDouble))
    })
    this.sqlContext.createDataFrame(segmented).toDF("label", "features")
  }
}
