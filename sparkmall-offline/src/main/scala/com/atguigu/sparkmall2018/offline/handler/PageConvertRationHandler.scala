package com.atguigu.sparkmall2018.offline.handler

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall2018.common.bean.UserVisitAction
import com.atguigu.sparkmall2018.common.util.{JdbcUtil, PropertiesUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession




object PageConvertRationHandler {
  def handle(sparkSession: SparkSession,userVisitActionRDD:RDD[UserVisitAction],taskId: String): Unit ={
    
    val properties: Properties = PropertiesUtil.load("condition.properties")
    val str: String = properties.getProperty("condition.params.json")
    val jsonObject: JSONObject = JSON.parseObject(str)
    val targetPageFlow: String = jsonObject.getString("targetPageFlow")
    val pageArray: Array[String] = targetPageFlow.split(",")
    val targetPageArray: Array[String] = pageArray.slice(0,pageArray.length-1)

    val targetPageArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageArray)
    val targetPageActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
      targetPageArrayBC.value.contains(userVisitAction.page_id.toString)
    }
//    targetPageActionRDD.foreach(println(_))
    //1.获取分母的值
    val pageVisitCountMap: collection.Map[Long, Long] = targetPageActionRDD
      .map{userVisitAction => (userVisitAction.page_id,1L)}.countByKey()
//    println(pageVisitCountMap.mkString("\n"))
    
    //2.获取分子的值
    //1,2,3,4,5,6 => 1-2,2-3,3-4,4-5,5-6,6-7
    val nextPageArray: Array[String] = pageArray.slice(1,pageArray.length)
    val pageJumpArray: Array[String] = targetPageArray.zip(nextPageArray).map{case (targetPage,nextPage)=>targetPage+"-"+nextPage}
    println(pageJumpArray.mkString(","))
    val pageJumpArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageJumpArray)
    
    //按照SessionId分组，按照时间排序，得到sessionid访问清单 1,2,4,5,7,12,34
    val userActionGroupBySessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD
      .map { userVisitAction => (userVisitAction.session_id, userVisitAction) }.groupByKey()


    val pageJumpRDD: RDD[String] = userActionGroupBySessionRDD.flatMap { case (sessionId, actionItr) =>
      val sortedActionsList: List[UserVisitAction] = actionItr.toList.sortWith(_.action_time < _.action_time)
      val pageIdList: List[Long] = sortedActionsList.map(_.page_id)
      val fromPageList: List[Long] = pageIdList.slice(0, pageIdList.length - 1)
      val toPageList: List[Long] = pageIdList.slice(1, pageIdList.length)

      //调整结构 1-2，2-4，4-5，5-7，7-12，12-34
      val pageJumpList: List[String] = fromPageList.zip(toPageList).map { case (fromPage, toPage) => fromPage + "-" + toPage }

      val filteredPageJumpList: List[String] = pageJumpList.filter { pageJump => pageJumpArrayBC.value.contains(pageJump) }
      filteredPageJumpList
    }
    pageJumpRDD.collect()
    val pageJumpCountMap: collection.Map[String, Long] = pageJumpRDD.map((_,1L)).countByKey()

    val pageJumpRatioList: Iterable[Array[Any]] = pageJumpCountMap.map { case (pageJump, count) =>
      val fromPageId: Long = pageJump.split("-")(0).toLong
      val pageVisitCount: Long = pageVisitCountMap.getOrElse(fromPageId, 100000L)
      val pageJumpRatio: Double = Math.round(count.toDouble * 1000 / pageVisitCount) / 10.0
      Array(taskId, pageJump, pageJumpRatio)

    }


    JdbcUtil.executeBatchUpdate("insert into page_convert_ratio values(?,?,?)",pageJumpRatioList)
    
  }

}
