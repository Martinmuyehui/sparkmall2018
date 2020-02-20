package com.atguigu.sparkmall2018.realtime.handler

import com.atguigu.sparkmall2018.common.util.RedisUtil
import org.json4s.JsonDSL._
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis


object AreaTop3AdsCountHandler {
  def handle(areaCityAdsDaycountDstream: DStream[(String,Long)])={

    //hmset(key,Map[String,String])
    //(area,(ads,count))=>Map[String,String]
    //area:ads:day count
    val areaAdsDayCountDstream: DStream[(String, Long)] = areaCityAdsDaycountDstream.map { case (areaCityAdsDay, count) =>
      val areaCityAdsDayArray: Array[String] = areaCityAdsDay.split(":")
      val area: String = areaCityAdsDayArray(0)
      val ads: String = areaCityAdsDayArray(2)
      val date: String = areaCityAdsDayArray(3)
      val areaAdsDay: String = area + ":" + ads + ":" + date
      (areaAdsDay, count)
    }.reduceByKey(_ + _)

    val  areaAdsCountGroupbyDateDstream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsDayCountDstream.map{case(areaAdsDay,count)=>
      val areaAdsDayArray: Array[String] = areaAdsDay.split(":")
      val area: String = areaAdsDayArray(0)
      val ads: String = areaAdsDayArray(1)
      val date: String = areaAdsDayArray(2)
      (date,(area,(ads,count)))
    }groupByKey()
    //大组整顿
    val areaAdsTop3CountGroupbyDateDstream: DStream[(String, Map[String, String])] = areaAdsCountGroupbyDateDstream.map { case (date, areaAdsCountItr) =>
      //按照地区进行分组，（area,Iterable(area,(ads,count))）//分小组
      val adsCountGroupbyArea: Map[String, Iterable[(String, (String, Long))]] = areaAdsCountItr.groupBy { case (area, (ads, count)) => area }

      //小组内做整顿
      val top3AdsCountJsonGroupbyAreaMap: Map[String, String] = adsCountGroupbyArea.map { case (area, areaAdsCountPerAreaItr) =>
        //把小组的地区字段去掉
        val adsCountItr: Iterable[(String, Long)] = areaAdsCountPerAreaItr.map { case (area, (ads, count)) =>
          (ads, count)
        }
        //排序，取前三
        val adsCountTop3List: List[(String, Long)] = adsCountItr.toList.sortWith(_._2 > _._2).take(3)

        //转json  由于fastjson gson jackson  只能转java对象  //JSON.toJSONString(adsCountTop3List)
        //所以要用scala专用的json工具  json4s
        val top3AdsJsonString: String = JsonMethods.compact(JsonMethods.render(adsCountTop3List))
        (area, top3AdsJsonString)
      }
      (date, top3AdsCountJsonGroupbyAreaMap)

    }

    //保存到redis中
    areaAdsTop3CountGroupbyDateDstream.foreachRDD{rdd=>

      rdd.foreachPartition{dateItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        dateItr.foreach{case (date,top3AdsCountJsonGroupbyAreaMap)=>
          val key: String = "top3_ads_per_day:"+date
          import collection.JavaConversions._
          jedis.hmset(key,top3AdsCountJsonGroupbyAreaMap)  //每天增加一个key
        }
        jedis.close()
      }

    }
  }

}
