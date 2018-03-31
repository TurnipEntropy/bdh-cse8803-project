package edu.gatech.cse8803.ETL

import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import scala.collections.mutable.ListBuffer
import com.cloudera.sparkts._
import scala.collection.mutable

object ETL {
  type SmallMap = mutable.Map[Long, Double]
  type InnerTuple = (Int, SmallMap, SmallMap)
  type MapKeyValue = (Timestamp, InnerTuple)
  type LargeMap = mutable.Map[Timestamp, InnerTuple]

  def grabFeatures(chartEvents: RDD[ChartEvent], gcsEvents: RDD[gcsEvent],
                    inOut: RDD[InOut], septicLables: RDD[SepticLabel],
                    allItemIds: RDD[Int]): RDD[(Long, MapKeyValue)] = {

    val emptyTimeSeries = createEmptyTimeSeries(inOut, allItemIds)
    mergeFeatureRDDs(chartEvents, gcsEvents, emptyTimeSeries)
  }

  def mergeFeatureRDDs(chartEvents: RDD[ChartEvent], gcsEvents: RDD[gcsEvent], emptyTimeSeries: RDD[((Long, Timestamp), Double)]):
  RDD[(Long, MapKeyValue)] = {
    val sc = chartEvents.context
    //first turn the gcsEvent into something that can be unioned with chartEvents
    //229000 = (max(itemid) in d_items / 1000 + 1) * 1000
    val itemedGcsEvents: RDD[ChartEvent] = gcsEvents.map(
      ge => ChartEvent(ge.patientId, ge.datetime, 229000, ge.gcsScore.toDouble)
    )
    val allEvents = sc.union(chartEvents, itemedGcsEvents)
    val keyedEvents = allEvents.map(
      evt => ((evt.patientId, evt.datetime), (evt.itemid, evt.value))
    )
    val keyedMapEvents = keyedEvents.combineByKey(
      (v) => SmallMap(v._1 -> v._2),
      (acc: SmallMap, v) => acc += (v._1 -> v._2),
      (acc1: SmallMap, acc2: SmallMap) => acc1 ++ acc2
    )

    val linkedMapEvents = emptyTimeSeries.join(keyedMapEvents)
    val splitMapEvents = linkedMapEvents.map({
      case (k, v) => (k._1, (k._2, v))
    })
    val createMapCombiner = (v: MapKeyValue) => {
      var map: LargeMap = mutable.Map()
      map(v._1) = (v._2._1, v._2._2, v._2._3)
      map
    }
    val mapCombiner = (acc: LargeMap, v: MapKeyValue) => {
      acc(v._1) = (v._2._1, v._2._2, v._2._3)
      acc
    }
    val mapBackwardMerge = (acc1: LargeMap, acc2: LargeMap) => {
      val combined: LargeMap = mutable.Map()
      val minKeyAcc1: Timestamp = acc1.keysIterator.min
      val minKeyAcc2: Timestamp = acc2.keysIterator.min
      val maxKeyAcc1: Timestamp = acc1.keysIterator.max
      val maxKeyAcc2: Timestamp = acc2.keysIterator.max
      val startTime = if (minKeyAcc1.before(minKeyAcc2)) minKeyAcc1 else minKeyAcc2
      var endTime = if (maxKeyAcc1.after(maxKeyAcc2)) maxKeyAcc1 else maxKeyAcc2
      //named earlier data because it's used that way in the loop! It is the end data, don't worry
      var earlierData = if (acc1.contains(startTime)) acc1(startTime) else acc2(startTime)
      val lastMap = earlierData._3
      val lastFullMap = earlierData._2
      for ((k, v) <- lastMap) {
        lastFullMap(k) = v
      }
      combined(endTime) = (earlierData._1, lastFullMap, lastMap)
      while (endTime.after(startTime)) {

        var midTime = new Timestamp(endTime.getTime - (1000 * 60 * 60))
        val laterData = earlierData
        earlierData= if (acc1.contains(midTime)) {
          acc1(midTime)
        } else {
          if (acc2.contains(midTime)) {
            acc2(midTime)
          } else {
            (-1, mutable.Map[Long, Double](), mutable.Map[Long, Double]())
          }
        }
        if (earlierData._1 != -1) {
          val earlierMap = earlierData._3
          val laterMap = laterData._3
          //this is the map that stores all instances of every measurement at every time
          val combinedFullMapLater = laterData._2
          val combinedFullMapEarlier = earlierData._2
          for ((k, v) <- laterMap) {
            if (!earlierMap.contains(k)) {
              earlierMap(k) = v
            }
          }
          for ((k, v) <- earlierMap){
            combinedFullMapEarlier(k) = v
          }
          combined(midTime) = (earlierData._1, combinedFullMapEarlier, earlierMap)
        }
        endTime = midTime
     }
      combined
    }
    val mapForwardMerge = (acc1: LargeMap, acc2: LargeMap) => {
      val combined: LargeMap = mutable.Map()
      val minKeyAcc1: Timestamp = acc1.keysIterator.min
      val minKeyAcc2: Timestamp = acc2.keysIterator.min
      val maxKeyAcc1: Timestamp = acc1.keysIterator.max
      val maxKeyAcc2: Timestamp = acc2.keysIterator.max
      var startTime = if (minKeyAcc1.before(minKeyAcc2)) minKeyAcc1 else minKeyAcc2
      val endTime = if (maxKeyAcc1.after(maxKeyAcc2)) maxKeyAcc1 else maxKeyAcc2
      //named later data because it's used that way in the loop! It is the start data, don't worry
      var laterData = if (acc1.contains(startTime)) acc1(startTime) else acc2(startTime)
      val firstMap = laterData._3
      val firstFullMap = laterData._2
      for ((k, v) <- firstMap) {
        firstFullMap(k) = v
      }
      combined(startTime) = (laterData._1, firstFullMap, firstMap)
      while (startTime.before(endTime)) {

        var midTime = new Timestamp(startTime.getTime + (1000 * 60 * 60))
        val earlierData = laterData
        laterData= if (acc1.contains(midTime)) {
          acc1(midTime)
        } else {
          if (acc2.contains(midTime)) {
            acc2(midTime)
          } else {
            (-1, mutable.Map[Long, Double](), mutable.Map[Long, Double]())
          }
        }
        if (laterData._1 != -1) {
          val earlierMap = earlierData._3
          val laterMap = laterData._3
          //this is the map that stores all instances of every measurement at every time
          val combinedFullMapLater = laterData._2
          val combinedFullMapEarlier = earlierData._2
          for ((k, v) <- earlierMap) {
            if (!laterMap.contains(k)) {
              laterMap(k) = v
            }
          }
          for ((k, v) <- laterMap){
            combinedFullMapLater(k) = v
          }
          combined(midTime) = (laterData._1, combinedFullMapLater, laterMap)
        }
        startTime = midTime
     }
      combined
    }

    val combinedMapEvents = splitMapEvents.combineByKey(
      createMapCombiner,mapCombiner,mapForwardMerge
    ).flatMapValues({
      case (timeMap) => for ((time, value) <- timeMap) yield (time, value)
    })

    combinedMapEvents.combineByKey(
      createMapCombiner, mapCombiner, mapBackwardMerge
    ).flatMapValues({
      case (timeMap) => for ((time, value) <- timeMap) yield (time, value)
    })
  }

  def createEmptyTimeSeries(inOut: RDD[InOut], allItemIds: RDD[Int]): RDD[((Long, Timestamp), mutable.Map[Long, Double])] = {
    val intermediate = inOut.map({
      case io => (io.patientId, createTimeList(io.intime, io.outtime))
    })
    //turn the v into part of the k, add a 0 (will act as the sepsis label later)
    val expanded = intermediate.flatMapValues(x => x).map({
      case (k,v) => ((k, v), 0)
    })
    val itemMap = scala.collection.mutable.Map(allItemIds.map(x => (x, 0.0)).collect: _*)
    expanded.map({
      case (k,v) => (k, (v, itemMap))
    })
  }

  def createTimeList(in: Timestamp, out: Timestamp): List[Timestamp] = {
    val timeList: ListBuffer[Timestamp] = new ListBuffer[Timestamp]()
    timeList += in
    var last = in
    while (last.before(out)) {
      last = new Timestamp(last.getTime + (1000 * 60 * 60))
      timeList += last
    }
    timeList.toList
  }

  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
}
