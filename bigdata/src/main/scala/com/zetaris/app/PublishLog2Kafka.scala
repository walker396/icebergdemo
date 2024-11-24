package com.zetaris.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.zetaris.data.bean.Order
import com.zetaris.data.storage.SalesReportIcebergTableManager.spark
import com.zetaris.utils.KafkaUtil

import scala.io.Source
import scala.jdk.CollectionConverters.asScalaBufferConverter


object PublishLog2Kafka {
  def main(args: Array[String]): Unit = {
    val resourcePath = getClass.getResource("/orders.json") // order log src/main/resources

    if (resourcePath == null) {
      println("Resource file not found!")
      return
    }
    val path = resourcePath.getPath
    println(s"Resource file path: $path")
    // parse the json
    val source = Source.fromFile(path)
    val jsonString = try source.mkString finally source.close()
    val testOrders: Seq[String] = Seq(
      """{"order_id":"ORD021","user_id":"USR001","product_id":"PRD001","product_name":"Wireless Mouse","category":"Electronics","price":25.99,"quantity":2,"order_date":"2024-11-15","order_status":"Completed","delivery_date":"2024-11-17"}""",
      """{"order_id":"ORD022","user_id":"USR002","product_id":"PRD002","product_name":"Bluetooth Keyboard","category":"Electronics","price":45.99,"quantity":1,"order_date":"2024-11-15","order_status":"Completed","delivery_date":"2024-11-18"}"""
    )
    testOrders.foreach(order => {
      println("*******" + order)
      sendMessage("ODS_ORDER_LOG",order)
    })
    val orders: Seq[Order] = JSON.parseArray(jsonString, classOf[Order]).asScala.toSeq
    val rand = new scala.util.Random
    for(i <- 0 until 3) {
      orders.foreach(order => {
        var newOrder = order.copy(order_id = rand.nextInt(1000).toString)
        sendMessage("ODS_ORDER_LOG", JSON.toJSONString(newOrder, new SerializeConfig(true)))

      })
      Thread.sleep(10000)

      spark.sql("REFRESH TABLE demo.orders")
      spark.sql("SELECT * FROM demo.orders ORDER BY updated_time DESC LIMIT 20;").show
    }

  }

  def sendMessage(topic: String, message: String) {
    val result =KafkaUtil.send(topic, message);
    println(result)
    KafkaUtil.flush
  }

}
