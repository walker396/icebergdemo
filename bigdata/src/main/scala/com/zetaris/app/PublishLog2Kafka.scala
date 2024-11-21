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
    val orders: Seq[Order] = JSON.parseArray(jsonString, classOf[Order]).asScala.toSeq
    val rand = new scala.util.Random
    for(i <- 0 until 1000) {
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
