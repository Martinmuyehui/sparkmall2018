package com.atguigu.sparkmall2018.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall2018.common.bean.UserVisitAction
import com.atguigu.sparkmall2018.common.util.{JdbcUtil, PropertiesUtil}
import com.atguigu.sparkmall2018.offline.acc.CategroyCountAccumulator
import com.atguigu.sparkmall2018.offline.bean.{CategoryCountInfo, TopSessionPerCid}
import com.atguigu.sparkmall2018.offline.handler.{CategoryCountHandler, PageConvertRationHandler, Top10categoryTopSessionHandler}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.{immutable, mutable}



object OfflineApp {


  private val taskId: String = UUID.randomUUID().toString

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OfflineApp").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val properties: Properties = PropertiesUtil.load("config.properties")

    val conditionsProp: Properties = PropertiesUtil.load("condition.properties")

    val conditionsJson: String = conditionsProp.getProperty("condition.params.json")

    val jSONObject: JSONObject = JSON.parseObject(conditionsJson)
  /*
  startDate:"2018-11-01", \
  endDate:"2018-12-28", \
  startAge: "20", \
  endAge: "50", \
  */
    val startDate: String = jSONObject.getString("startDate")
    val endDate: String = jSONObject.getString("endDate")
    val startAge: String = jSONObject.getString("startAge")
    val endAge: String = jSONObject.getString("endAge")

    val database: String = properties.getProperty("hive.database")

    sparkSession.sql("use "+database)

    var sql="select uv.* from user_visit_action uv join user_info ui on uv.user_id=ui.user_id where 1=1"

    if (startDate!=null && startDate.length>0){
      sql += " and uv.date >= '" + startDate + "'"
    }
    if (endDate != null && endDate.length>0) {
      sql +=" and uv.date <= '"+endDate+"'"
    }
    if (startAge != null) {
      sql +=" and ui.age >="+startAge
    }
    if (endAge != null) {
      sql+=" and ui.age >=" +endAge
    }

//    val frame: DataFrame = sparkSession.sql(sql)
//    val unit: Dataset[UserVisitAction] = frame.as[UserVisitAction]
//    val rdd: RDD[UserVisitAction] = unit.rdd
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd
//需求一
//    val categoryCountTop10List: List[CategoryCountInfo] = CategoryCountHandler.handle(sparkSession,rdd,taskId)
//    println("需求一完成")
//
////需求二
//    Top10categoryTopSessionHandler.handler(sparkSession,rdd,categoryCountTop10List,taskId)
//    println("需求二完成")

//需求四
    PageConvertRationHandler.handle(sparkSession,rdd,taskId)
    println("需求四完成")
  }



}
