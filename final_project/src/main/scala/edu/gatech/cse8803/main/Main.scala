/**
 *
 */

package edu.gatech.cse8803.main

import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
import org.postgresql._
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._

object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)
    /*val dbc = "jdbc:postgresql://hostmachine:9530/postgres?user=postgres&password=Password123"
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(dbc)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    try {
      val prep = conn.prepareStatement("SELECT * FROM admissions WHERE subject_id = 61")
      val rs: ResultSet = prep.executeQuery()
    }*/

    sc.stop()
  }

  def loadLocalRddRawData(sqlContext: SQLContext): (RDD[ChartEvent], RDD[GCSEvent], RDD[InOut], RDD[SepticLabel]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val homeString = "file:///home/bdh/project/"
    List(
      homeString + "gcs_data.csv",
      homeString + "in_out.csv",
      homeString + "metavision_data.csv",
      homeString + "septic_id_timestamp.csv"
    ).foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val chartEvents: RDD[ChartEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, itemid, avg_value
        |FROM metavision_data
      """.stripMargin).
      map(r => ChartEvent(r(0).toString.toLong,
                          new Timestamp(dateFormat.parse(r(1).toString).getTime),
                          r(2).toString.toLong,
                          r(3).toString.toDouble
                          )
      )//.cache()
    val gcsEvents: RDD[GCSEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, gcs
        |FROM gcs_data
      """.stripMargin).
      map(r => GCSEvent(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                        r(2).toString.toInt))//.cache()

    val inOut: RDD[InOut] = sqlContext.sql(
      """
        |SELECT subject_id, intime, outtime
        |FROM in_out
      """.stripMargin
    ).map( r => InOut(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                      new Timestamp(dateFormat.parse(r(2).toString).getTime)))//.cache()

    val septicLabels: RDD[SepticLabel] = sqlContext.sql(
      """
        |SELECT *
        |FROM septic_id_timestamp
      """.stripMargin
    ).map( r => SepticLabel(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime)))//.cache()
    (chartEvents, gcsEvents, inOut, septicLabels)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
