package com.atguigu.sparkmall2018.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall2018.common.util.RedisUtil
import com.atguigu.sparkmall2018.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdscountHandler {
  def handle(filteredAdslogDstrea:DStream[AdsLog],sparkContext: SparkContext)={
    //map => key: area:city:ads:date  value:1L
    val areaCityAdsDateDstream: DStream[(String, Long)] = filteredAdslogDstrea.map { adsLog =>
      val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(adsLog.ts))
      val key: String = adsLog.area + ":" + adsLog.city + ":" + adsLog.adsId + ":" + date
      (key, 1L)
    }
    sparkContext.setCheckpointDir("./checkpoint")
    val areCityAdsDaycountDstream: DStream[(String, Long)] = areaCityAdsDateDstream.updateStateByKey { (valueSeq: Seq[Long], totalOption: Option[Long]) =>
      println(valueSeq.mkString(","))
      val sum: Long = valueSeq.sum
      val total: Long = totalOption.getOrElse(0L) + sum
      Some(total)
    }
    //把流中的结果刷新到redis中
    //driver中执行 程序启动是只启动一次
    areCityAdsDaycountDstream.foreachRDD{rdd=>
    //driver中执行，一个周期执行一次
      rdd.foreachPartition{areaCityAdsCountItr=>
    //executor  每个分区执行一次
        val key = "area_city_ads_daycount"
        val areaCityAdsCountMap: Map[String, String] = areaCityAdsCountItr.map{case (key,count)=>
          (key,count.toString)  //每个元素执行一次
        }.toMap

        //由于jedis支持的是java的map，所以引用隐式转换把scala的map转换成java的map
        import scala.collection.JavaConversions._

        if (areaCityAdsCountMap!=null&&areaCityAdsCountMap.size>0){
          val jedis: Jedis = RedisUtil.getJedisClient
          jedis.hmset(key,areaCityAdsCountMap)
          jedis.close()
        }
      }
    }
    areCityAdsDaycountDstream
  }

}
