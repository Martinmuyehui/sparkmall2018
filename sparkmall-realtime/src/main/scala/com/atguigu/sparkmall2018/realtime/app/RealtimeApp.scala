package com.atguigu.sparkmall2018.realtime.app

import com.atguigu.sparkmall2018.common.util.MyKafkaUtil
import com.atguigu.sparkmall2018.realtime.bean.{AdsLog, BlacklistHandler}
import com.atguigu.sparkmall2018.realtime.handler.{AreaCityAdscountHandler, AreaTop3AdsCountHandler, OneHourAdsCountHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RealtimeApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(new SparkContext(conf),Seconds(5))
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)

    val adsLogDstream: DStream[AdsLog] = recordDstream.map { record =>

      val adsLogString: String = record.value()
      val adsLogArray: Array[String] = adsLogString.split(" ")
      AdsLog(adsLogArray(0).toLong, adsLogArray(1), adsLogArray(2), adsLogArray(3).toLong, adsLogArray(4).toLong)
    }

    //需求五  过滤黑名单
    val filteredAdslogDstream: DStream[AdsLog] = BlacklistHandler.checkBlackList(ssc.sparkContext,adsLogDstream)
    filteredAdslogDstream.cache()
    //需求五  更新点击量
    BlacklistHandler.updateUserAdsCount(filteredAdslogDstream)

    //需求六  每天各地区各城市各广告的点击量
    val areaCityAdsDaycountDstream: DStream[(String, Long)] = AreaCityAdscountHandler.handle(filteredAdslogDstream,ssc.sparkContext)
    recordDstream.foreachRDD{rdd =>
      println(rdd.map(_.value()).collect().mkString("\n"))
    }

    //需求七
    AreaTop3AdsCountHandler.handle(areaCityAdsDaycountDstream)

    //需求八
    OneHourAdsCountHandler.handle(filteredAdslogDstream)
    ssc.start()
    ssc.awaitTermination()
  }
}
