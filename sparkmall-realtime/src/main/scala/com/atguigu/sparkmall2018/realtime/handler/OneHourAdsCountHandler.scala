package com.atguigu.sparkmall2018.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall2018.common.util.RedisUtil
import org.json4s.JsonDSL._
import com.atguigu.sparkmall2018.realtime.bean.AdsLog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object OneHourAdsCountHandler {

  def handle(filteredAdslogDstream: DStream[AdsLog])={

    val lastHourAdsLogDstream: DStream[AdsLog] = filteredAdslogDstream.window(Minutes(60),Seconds(10))

// (ads_hourMinute,count) => (ads,(hourMinute,count))
    val adsHourMinteCountDstream: DStream[(String, Long)] = lastHourAdsLogDstream.map { adsLog =>
      val hourMinute: String = new SimpleDateFormat("HH:mm").format(new Date(adsLog.ts))
      val ads: Long = adsLog.adsId
      val key: String = ads + "_" + hourMinute
      (key, 1L)
    }.reduceByKey(_ + _)

    //(ads,(hourMinute,count))
    val hourMinuteCountGroupbyAdsDtream: DStream[(String, Iterable[(String, Long)])] = adsHourMinteCountDstream.map { case (ads_hourMinute, count) =>
      val ads_hourMinuteArray: Array[String] = ads_hourMinute.split("_")
      val ads: String = ads_hourMinuteArray(0)
      val hourMinute: String = ads_hourMinuteArray(1)
      (ads, (hourMinute, count))
    }.groupByKey()

    //(hourMinute, count)=>{Iterable(hourMinute, count)}  //json格式
    val adsHourMinuteCountDstream: DStream[(String, String)] = hourMinuteCountGroupbyAdsDtream.map { case (ads, hourMinuteItr) =>
      val hourMinuteCountJsonString: String = JsonMethods.compact(JsonMethods.render(hourMinuteItr))
      (ads, hourMinuteCountJsonString)
    }

    //保存到redis中
    adsHourMinuteCountDstream.foreachRDD{rdd=>

//      val key = "last_hour_ads_click"
//      import collection.JavaConversions._
//      val adsItrMap: Map[String, String] = rdd.collect().toMap
//
//      if (adsItrMap != null && adsItrMap.size > 0) {
//
//        val jedis: Jedis = RedisUtil.getJedisClient
//        jedis.hmset(key, adsItrMap)
//        jedis.close()
//      }

      rdd.foreachPartition { adsItr =>
        //如果使用adsItr.size判断之后，adsTtr将没有值，必须先用.toMap和.toList把它保存到内存中
        val adsItrMap: Map[String, String] = adsItr.toMap
        val key = "last_hour_ads_click"

        if (adsItrMap != null && adsItrMap.size > 0) {
          val jedis: Jedis = RedisUtil.getJedisClient

          import collection.JavaConversions._
          jedis.hmset(key, adsItrMap)
          jedis.close()
        }
      }

    }
  }

}
