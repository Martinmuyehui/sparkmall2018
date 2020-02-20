package com.atguigu.sparkmall2018.offline.udf


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class CityRatioUDAF extends UserDefinedAggregateFunction{
  //输入字段声名
  override def inputSchema: StructType = StructType(Array(StructField("cityName",StringType)))

  //声名存储的容器
  override def bufferSchema: StructType = StructType(Array(StructField("countMap",MapType(StringType,LongType)),
    StructField("counSum",LongType)))

  //输出类型
  override def dataType: DataType = StringType

  //校验一致性
  override def deterministic: Boolean = true

  //初始化容器
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=new HashMap[String,Long]()
    buffer(1)=0L
  }

  //更新数据 把传入值存入Map中
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val countMap: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val countSum: Long = buffer.getLong(1)
    val cityName: String = input.getString(0)

    buffer(0)=countMap+(cityName -> (countMap.getOrElse(cityName,0L)+1L))
    buffer(1)=countSum+1L
  }

  //分区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val countMap1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val countSum1: Long = buffer1.getLong(1)
    val countMap2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    val countSum2: Long = buffer2.getLong(1)

    buffer1(0)=countMap1.foldLeft(countMap2){case (countMap2,(cityName,count))=>
      countMap2 + (cityName -> (countMap2.getOrElse(cityName,0L)+count))
    }
    buffer1(1)=countSum1+countSum2
  }

  //最终显示结果 (cityName,count),countSum
  override def evaluate(buffer: Row): String = {
    val countMap: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val countSum: Long = buffer.getLong(1)
    val cityName: Iterable[String] = countMap.keys

    //排序，取前两个(cit,count)
    val top2CityCount: List[(String, Long)] = countMap.toList.sortWith(_._2>_._2)take(2)

    //计算百分比
    var cityInfoTop2List: List[CityRateInfo] = top2CityCount.map { case (cit, count) =>
      val cityRatio: Double = Math.round(count * 1000 / countSum) / 10.0
      CityRateInfo(cit, cityRatio)
    }

    //计算其他的百分比
    var otherRatio=100.0

    //方法二
//    val count2Sum: Double = cityInfoTop2List.map(_.rate).reduce(_+_)
//    val otherRate: Double = Math.round((100.0-count2Sum)*10)/10.0

    for (elem <- cityInfoTop2List) {
      otherRatio -= elem.rate
    }
    otherRatio = Math.round(otherRatio*10)/10.0

    //把cityName和count拼接
    cityInfoTop2List = cityInfoTop2List :+ CityRateInfo("其他",otherRatio)

    cityInfoTop2List.mkString(",")

  }
}
case class CityRateInfo(cityName:String,rate:Double){
  override def toString: String = {
    cityName+":"+rate+"%"
  }
}