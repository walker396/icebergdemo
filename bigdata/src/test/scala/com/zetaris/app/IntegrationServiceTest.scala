package com.zetaris.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.zetaris.untils.IcebergUtil
import org.slf4j.LoggerFactory
import com.zetaris.data.bean.Order
import com.zetaris.utils.KafkaUtil
import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.util.Date

class IntegrationServiceTest extends AnyFunSuite with BeforeAndAfter {
  private val logger = LoggerFactory.getLogger(this.getClass)

  var spark: SparkSession = _
  var kafkaTopic = "ODS_ORDER_LOG"
  val testOrders: Seq[String] = Seq(
    """{"order_id":"ORD021","user_id":"USR001","product_id":"PRD001","product_name":"Wireless Mouse","category":"Electronics","price":25.99,"quantity":2,"order_date":"2024-11-15","order_status":"Pending","delivery_date":"2024-11-17"}""",
    """{"order_id":"ORD022","user_id":"USR002","product_id":"PRD002","product_name":"Bluetooth Keyboard","category":"Electronics","price":45.99,"quantity":1,"order_date":"2024-11-15","order_status":"Completed","delivery_date":"2024-11-18"}"""
  )


  before {
    val config = IntegrationService.parseConfig("")
    spark = IcebergUtil.initializeSparkSession(config)
    logger.info("Starting Kafka" + new Date())
    KafkaUtil.configure("")
    //    KafkaUtil.deleteTopic(kafkaTopic)
    //    KafkaUtil.createTopic(kafkaTopic)
    //    FileUtils.deleteDirectory(new File("src/main/resources/warehouse/catalog/demo"))
    logger.info("clean the testing data in iceberg")
        spark.sql("DELETE FROM demo.orders WHERE order_id IN " +
          "('ORD021', 'ORD022','ORD041','ORD042')")

  }

  after {

    logger.info("Test completed")
  }


  test("Test Insert 2 new orders") {

    logger.info("----start testing----" + new Date())

    testOrders.foreach(order => {
      sendMessage(order)
    })
    Thread.sleep(20000)

    val table: Table = Spark3Util.loadIcebergTable(spark, "demo.orders")
    table.refresh()
    val insertCount = spark.sql("SELECT COUNT(*) as orderCount FROM demo.orders where order_id in ('ORD021','ORD022')").first().getAs[Long]("orderCount")
    assert(insertCount == 2, "After insertion, there should be 2 records.")

  }

  test("Test Insert one duplicated order") {
    logger.info("----start testing----" + new Date())
    var duplicatedOrder: Order = new Order("ORD021", "USR001", "PRD001", "Wireless Mouse", "Electronics", 25.99,
      2, "2024-11-15", "Pending", "2024-11-17")


    sendMessage(JSON.toJSONString(duplicatedOrder, new SerializeConfig(true)))
    Thread.sleep(20000)
    val table: Table = Spark3Util.loadIcebergTable(spark, "demo.orders")
    table.refresh()
    val insertCount = spark.sql("SELECT COUNT(*) as orderCount FROM demo.orders where order_id in ('ORD021')").first().getAs[Long]("orderCount")
    assert(insertCount == 1, "only one record")
  }
  test("Test update the status of one order") {
    var updatedOrder: Order = new Order("ORD021", "USR001", "PRD001", "Wireless Mouse", "Electronics", 25.99,
      2, "2024-11-15", "Completed", "2024-11-17")
    sendMessage(JSON.toJSONString(updatedOrder, new SerializeConfig(true)))
    Thread.sleep(20000)
    val table: Table = Spark3Util.loadIcebergTable(spark, "demo.orders")
    table.refresh()
    val orderStatus = spark.sql("SELECT order_status FROM demo.orders where order_id = 'ORD021'").first().getAs[String]("order_status")
    assert(orderStatus == "Completed", "The order status should change to Completed")
  }

  test("Test Event Sequential Consistency") {
    val seqOrders: Seq[String] = Seq(
      """{"order_id":"ORD041","user_id":"USR003","product_id":"PRD003","product_name":"Wireless Mouse3","category":"Electronics","price":125.99,"quantity":1,"order_date":"2024-11-17","order_status":"Pending","delivery_date":"2024-11-25"}""",
      """{"order_id":"ORD042","user_id":"USR004","product_id":"PRD004","product_name":"Bluetooth Keyboard4","category":"Electronics","price":145.99,"quantity":1,"order_date":"2024-11-17","order_status":"Pending","delivery_date":"2024-11-25"}"""
    )
    seqOrders.foreach(sendMessage)
    Thread.sleep(20000)
    val table: Table = Spark3Util.loadIcebergTable(spark, "demo.orders")
    table.refresh()
    val eventOrders = spark.sql("SELECT order_id FROM demo.orders ORDER BY updated_time DESC limit 2")
      .collect().map(_.getString(0)).toSeq
    assert(eventOrders == Seq("ORD041", "ORD042"), "Events sequencing should be consistent.")
  }

  test("Test Data Integrity") {
    Thread.sleep(10000)
    val matchingRecords = spark.sql(
      "SELECT COUNT(*) as orderCount FROM demo.orders WHERE order_id = 'ORD041' AND product_name = 'Wireless Mouse3'"
    ).first().getAs[Long]("orderCount")
    assert(matchingRecords == 1, "The data fields should match exactly.")
  }


  def sendMessage(message: String) {
    val topic = "ODS_ORDER_LOG"
    val result = KafkaUtil.send(topic, message);
    logger.info(message)
    logger.info("######")
    KafkaUtil.flush
  }
}
