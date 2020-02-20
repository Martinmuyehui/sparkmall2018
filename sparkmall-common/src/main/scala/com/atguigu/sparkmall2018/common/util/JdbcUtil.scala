package com.atguigu.sparkmall2018.common.util

import java.sql.PreparedStatement
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JdbcUtil {

  var dataSource: DataSource = init()

  def init() = {

    val config = PropertiesUtil.load("config.properties")

    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))

    DruidDataSourceFactory.createDataSource(properties)

  }

  def executeUpdate(sql: String, params: Array[Any]): Int = { // "insert into xxx values (?,?,?)"
    var rtn = 0
    var pstmt: PreparedStatement = null
    val connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }



  def executeBatchUpdate(sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    val connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      rtn = pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  def main(args: Array[String]): Unit = {
    JdbcUtil.executeUpdate("insert into category_top10 values(?,?,?,?,?)", Array("task1","12",100,123, 12 ) )
  }
}
