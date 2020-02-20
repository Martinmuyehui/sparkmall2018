package com.atguigu.sparkmall2018.offline.app

import java.util.Properties

import com.atguigu.sparkmall2018.common.util.PropertiesUtil
import com.atguigu.sparkmall2018.offline.udf.CityRatioUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AreaTop3ProductApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AreaTop3ProductApp").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val properties: Properties = PropertiesUtil.load("config.properties")

    val database: String = properties.getProperty("hive.database")

    sparkSession.udf.register("city_remark",new CityRatioUDAF)

    sparkSession.sql("use "+database)

    sparkSession.sql("select ci.area,ci.city_name,uv.click_product_id from user_visit_action uv  join  city_info ci  on  uv.city_id=ci.city_id where uv.click_product_id>0")
      .createOrReplaceTempView("t1")

    sparkSession.sql("select area,click_product_id,count(*) clicount,city_remark(city_name) remark from t1 group by area,click_product_id")
      .createOrReplaceTempView("t2")

    sparkSession.sql("select *,row_number()over(partition by area order by clicount desc) rn from t2")
      .createOrReplaceTempView("t3")

    sparkSession.sql("select t3.area,pin.product_name,t3.clicount,t3.remark from t3,product_info pin where rn<=3 and t3.click_product_id=pin.product_id")
      .show(100,false)


  }

}
