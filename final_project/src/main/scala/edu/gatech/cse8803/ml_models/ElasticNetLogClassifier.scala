import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, LongType, DoubleType, IntType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.lang.{Double => jDouble}
import edu.gatech.cse8803.main.Main.{PatientTuple, KeyTuple, ValueTuple}

class ElasticNetLogClassifier (elasticNetParam: Double = 0.15, fitIntercept: Boolean = True,
                               maxIter: Int = 10, standardize: Boolean = False,
                               threshold: Double = 0.1){
  var model: LogisticRegression = new LogisticRegression().setMaxIter(maxIter).
                                                           setElasticNetParam(elasticNetParam).
                                                           setFitIntercept(fitIntercept).
                                                           setStandardization(standardize).
                                                           setThreshold(threshold)

  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

  def train(data: RDD[(KeyTuple, ValueTuple)]) =  {
    //have to turn it into a DataFrame, with the first entry being the label
    //and the rest being the data.
    //k,v = (Long, Long), (Timestamp, Int, SummedGcsPatient)

    val segmented = data.map({
      case (k, v) => (v._2, v._3.bpDia, v._3.bpSys, v._3.heartRate, v._3.respRate,
                      v._3.temp, v._3.spo2, v._3.gcs, v._3.age)
    })
    val training = spark.createDataFrame(segmented).toDF("label", "diastolic", "systolic", "heartRate",
                                          "respRate", "temperature", "spo2", "gcs", "age")

    model.fit(training)

  }
}
