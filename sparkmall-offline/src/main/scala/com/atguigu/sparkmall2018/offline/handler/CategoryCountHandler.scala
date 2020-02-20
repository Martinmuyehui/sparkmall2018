package com.atguigu.sparkmall2018.offline.handler

import com.atguigu.sparkmall2018.common.bean.UserVisitAction
import com.atguigu.sparkmall2018.common.util.JdbcUtil
import com.atguigu.sparkmall2018.offline.acc.CategroyCountAccumulator
import com.atguigu.sparkmall2018.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

object CategoryCountHandler {

  def handle(sparkSession: SparkSession,userVisitActionRDD:RDD[UserVisitAction],taskId:String): List[CategoryCountInfo] ={
    val accumulator = new CategroyCountAccumulator

    sparkSession.sparkContext.register(accumulator)

    userVisitActionRDD.foreach{userVisitAction=>
      if (userVisitAction.click_category_id!= -1L) {
        accumulator.add(userVisitAction.click_category_id+"_click")
      }else if (userVisitAction.order_category_ids!= null) {
        val cidArray: Array[String] = userVisitAction.order_category_ids.split(",")
        for (cid <- cidArray) {
          accumulator.add(cid+"_order")
        }
      }else if (userVisitAction.pay_category_ids!= null) {
        val cidArray: Array[String] = userVisitAction.pay_category_ids.split(",")
        for (cid <- cidArray) {
          accumulator.add(cid+"_pay")
        }

      }
    }

    val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value

    println(categoryCountMap.mkString("\n"))

    val countGroupbyCidMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy{case (cid_action,count)=> cid_action.split("_")(0)}

    val categoryCountItr: immutable.Iterable[CategoryCountInfo] = countGroupbyCidMap.map { case (cid, countMap) =>
      CategoryCountInfo(taskId, cid, countMap.getOrElse(cid + "_click", 0L), countMap.getOrElse(cid + "_order", 0L), countMap.getOrElse(cid + "_pay", 0L))
    }

    //取前十
    val sortedCategoryCountInfoList: List[CategoryCountInfo] = categoryCountItr.toList.sortWith { (countArray1, countArray2) =>
      if (countArray1.clickCount > countArray2.clickCount) {
        true
      } else if (countArray1.clickCount == countArray2.clickCount) {
        if (countArray1.orderCount > countArray2.orderCount) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }
    val top10CategoryList: List[CategoryCountInfo] = sortedCategoryCountInfoList.take(10)

    val top10CategoryParam: List[Array[Any]] = top10CategoryList.map { categoryInfo =>
      Array(categoryInfo.taskId, categoryInfo.cid, categoryInfo.clickCount, categoryInfo.orderCount, categoryInfo.payCount)
    }

    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",top10CategoryParam)

    top10CategoryList
  }
}
