package com.atguigu.sparkmall2018.offline.handler

import com.atguigu.sparkmall2018.common.bean.UserVisitAction
import com.atguigu.sparkmall2018.common.util.JdbcUtil
import com.atguigu.sparkmall2018.offline.bean.{CategoryCountInfo, TopSessionPerCid}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Top10categoryTopSessionHandler {

  def handler(sparkSession:SparkSession, rdd:RDD[UserVisitAction], categoryCountTop10List:List[CategoryCountInfo],taskId:String) ={
    val topCategoryListBC: Broadcast[List[CategoryCountInfo]] = sparkSession.sparkContext.broadcast(categoryCountTop10List)
    val topCidActionRDD: RDD[UserVisitAction] = rdd.filter { userVisitAction =>
      val topicdList: List[Long] = topCategoryListBC.value.map(categoryTopN =>
        categoryTopN.cid.toLong)
//      println(topicdList.mkString("-") + "|||" + userVisitAction.click_category_id)
      topicdList.contains(userVisitAction.click_category_id)
    }
//    topCidActionRDD.foreach(println(_))

    //(cid_sessionId,1L)=>(cid_sessionId,count)
    val topCidSessionClickRDD: RDD[(String, Long)] = topCidActionRDD.map { userVisitAction =>
      (userVisitAction.click_category_id + "_" + userVisitAction.session_id, 1L)
    }
    val topCidSessionCountRDD: RDD[(String, Long)] = topCidSessionClickRDD.reduceByKey(_+_)

    //(cid_sessionId,count)=>(cid,TopSessionPerCid(taskId, cid, sessionId, count))
    val sessionCountByCidRDD: RDD[(String, Iterable[TopSessionPerCid])] = topCidSessionCountRDD.map { case (cid_sessionId, count) =>
      val cid: String = cid_sessionId.split("_")(0)
      val sessionId: String = cid_sessionId.split("_")(1)
      (cid, TopSessionPerCid(taskId, cid, sessionId, count))
    }.groupByKey()

    //  10 * 10                                            =>      100
    //(cid,TopSessionPerCid(taskId, cid, sessionId, count))=>TopSessionPerCid(taskId, cid, sessionId, count)
    val top10SessionPerCidRDD: RDD[Array[Any]] = sessionCountByCidRDD.flatMap { case (cid, topsessionItr) =>
      val top10SessionCountList: List[TopSessionPerCid] = topsessionItr.toList.sortWith(_.clickCount > _.clickCount).take(10)
      val topListArray: List[Array[Any]] = top10SessionCountList.map{topSessionPerCid=>
        Array(topSessionPerCid.taskId,topSessionPerCid.category_id,topSessionPerCid.sessionId,topSessionPerCid.clickCount)}
      topListArray
    }

    val top10SessionListForSave: List[Array[Any]] = top10SessionPerCidRDD.collect().toList


    JdbcUtil.executeBatchUpdate("insert into top10_session value(?,?,?,?)",top10SessionListForSave)
  }
}
