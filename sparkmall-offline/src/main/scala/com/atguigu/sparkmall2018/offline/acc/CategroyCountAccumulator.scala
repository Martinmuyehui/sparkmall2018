package com.atguigu.sparkmall2018.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategroyCountAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

  private var categoryCountMap = new mutable.HashMap[String,Long]()

  //判空
  override def isZero: Boolean = categoryCountMap.isEmpty

  //拷贝
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategroyCountAccumulator
    accumulator.categoryCountMap ++= categoryCountMap
    accumulator
  }

  override def reset(): Unit = categoryCountMap.clear()

  override def add(v: String): Unit = {

    categoryCountMap(v) = categoryCountMap.getOrElse(v,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value

    categoryCountMap = categoryCountMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count
      otherMap
    }

  }

  override def value: mutable.HashMap[String, Long] = {
    this.categoryCountMap
  }
}
