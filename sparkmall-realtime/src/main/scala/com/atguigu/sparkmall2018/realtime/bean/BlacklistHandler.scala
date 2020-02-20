package com.atguigu.sparkmall2018.realtime.bean

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.sparkmall2018.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlacklistHandler {



  /*
  * 更新用户每天点击广告计数
  * */
  def updateUserAdsCount(adsLogDstream:DStream[ AdsLog])={
    adsLogDstream.foreachRDD{rdd =>

      //设计Redis键值：user_ads_daycount  hash结构 field  udi:ads:date  value:count
//      rdd.foreach{adslog =>
//        val jedis: Jedis = RedisUtil.getJedisClient  //建立连接太频繁
//        val key="user_ads_daycount"
//        val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(adslog.ts))
//        val field= adslog.uid+":"+adslog.adsId+":"+date
//        jedis.hincrBy(key,field,1L)

      rdd.foreachPartition{adslogItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        val key: String = "user_ads_daycount"
        val blackListKey ="blacklist"

          adslogItr.foreach { adslog =>

          val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(adslog.ts))
          val field: String = adslog.uid+":"+adslog.adsId+":"+date
          jedis.hincrBy(key,field,1L)
          val countStr: String = jedis.hget(key,field)
            //达到100点击量   保存进黑名单
          if (countStr.toLong>= 100) {
            jedis.sadd(blackListKey,adslog.uid.toString)
          }
        }
        jedis.close()

      }

    }
  }
  def checkBlackList(sparkContext: SparkContext,adsLogDstream:DStream[ AdsLog]): DStream[AdsLog] ={
//    val filterDstream: DStream[AdsLog] = adsLogDstream.filter { adslog =>
//      val jedis: Jedis = RedisUtil.getJedisClient
//      !jedis.sismember("blacklist", adslog.uid.toString)
//    }

//    val jedis: Jedis = RedisUtil.getJedisClient  //只执行一次 driverzhong
//    val blackList: util.Set[String] = jedis.smembers("blacklist")
//    val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackList)

    val filterDstream: DStream[AdsLog] = adsLogDstream.transform { rdd =>
      val jedis: Jedis = RedisUtil.getJedisClient    //每隔一个批次执行一次 driver中
      val blackList: util.Set[String] = jedis.smembers("blacklist")
      //每个固定周期从redis中取到最新的黑名单  通过广播变量发送给executor
      val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackList)

      //executor 根据广播变量的黑名单进行过滤
      val filterRDD: RDD[AdsLog] = rdd.filter { adslog =>
        !blacklistBC.value.contains(adslog.uid.toString)   //executor执行
      }
      filterRDD
    }
    filterDstream


  }

}
