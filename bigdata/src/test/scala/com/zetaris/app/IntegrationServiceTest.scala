package com.zetaris.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import com.zetaris.config.Config
import com.zetaris.data.bean.Order
import com.zetaris.untils.PropertiesUtil
import com.zetaris.utils.KafkaUtil
import org.apache.commons.io.FileUtils
import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.utility.DockerImageName
import com.zetaris.data.storage.SalesReportIcebergTableManager.spark
import java.io.File
import java.util.{Date, Properties}
import java.util.concurrent.{ExecutorService, Executors}

class IntegrationServiceTest extends AnyFunSuite with BeforeAndAfter {

//  override val container: KafkaContainer = KafkaContainer()
//  private lazy val bootstrapServers = container.bootstrapServers

//  var spark: SparkSession = _
  var executor: ExecutorService = _
  var kafkaTopic = "ODS_ORDER_LOG"
  val testOrders: Seq[String] = Seq(
    """{"order_id":"ORD021","user_id":"USR001","product_id":"PRD001","product_name":"Wireless Mouse","category":"Electronics","price":25.99,"quantity":2,"order_date":"2024-11-15","order_status":"Pending","delivery_date":"2024-11-17"}""",
    """{"order_id":"ORD022","user_id":"USR002","product_id":"PRD002","product_name":"Bluetooth Keyboard","category":"Electronics","price":45.99,"quantity":1,"order_date":"2024-11-15","order_status":"Completed","delivery_date":"2024-11-18"}"""
  )



  val config = IntegrationService.parseConfig()
  var query: StreamingQuery = null
  val spark = IntegrationService.spark;
  before {
    println("Starting Kafka"+ new Date())
//    KafkaUtil.deleteTopic(kafkaTopic)
//    KafkaUtil.createTopic(kafkaTopic)
//    FileUtils.deleteDirectory(new File("src/main/resources/warehouse/catalog/demo"))
//    container.start()

//    executor = Executors.newSingleThreadExecutor()

//    val task: Runnable = new Runnable {
//      def run(): Unit = {
//        IntegrationService.main(Array("test"));
////        IntegrationService.main(null);
//      }
//    }
//    executor.submit(task)


    println("init the SparkSession")

//
//    spark = IntegrationService.initializeSparkSession(config)
//    IntegrationService.ensureStorageTableExists(spark)
//    IntegrationService.ensureTempTableExists(spark)
//    query = IntegrationService.processOrderSummaryStream(spark, config)
//    query.awaitTermination()
  }

  after {
//    println("Stopping Kafka")
//    container.stop()
    // 主程序执行完毕后关闭线程池
//    executor.shutdown()
  }



  test("Test Insert 2 new orders") {

    println("----start testing----"+ new Date())
//    val config = IntegrationService.StorageConfig(
//      container.bootstrapServers,
//      "ODS_ORDER_LOG"
//    )

    testOrders.foreach(order => {
      println("*******"+order)
      sendMessage(order)
    })
    val rand = new scala.util.Random
    val categories = Array("Electronics","Furniture")
//    Thread.sleep(300000) // 等待数据流处理
    //push more data to trigger Iceberg to store data in file.
//    for (i <- 0 until 30) {
//        var newOrder = otherTemplateOrders.copy(order_id = "ORD"+rand.nextInt(1000).toString, user_id = rand.nextInt(1000).toString,category = categories(i%categories.length))
//        sendMessage(JSON.toJSONString(newOrder, new SerializeConfig(true)))
//
//      Thread.sleep(10000)
//    }
    Thread.sleep(20000)
//    // 1. 测试插入
//    spark.sql("REFRESH TABLE demo.orders")
//    val table = spark.read.format("iceberg").load("demo.orders")
//    spark.sql("SELECT * FROM demo.orders").show()
    val table: Table = Spark3Util.loadIcebergTable(spark, "demo.orders")
    table.refresh()
    val insertCount = spark.sql("SELECT COUNT(*) as orderCount FROM demo.orders where order_id in ('ORD021','ORD022')").first().getAs[Long]("orderCount")
    assert(insertCount == 2, "After insertion, there should be 2 records.")

  }

  test("Test Insert one duplicated order") {
    val spark = IntegrationService.spark;
    println("----start testing----" + new Date())
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
    val eventOrders = spark.sql("SELECT order_id FROM demo.orders ORDER BY updated_time DESC limit 2")
      .collect().map(_.getString(0))
    assert(eventOrders.toSeq == Seq("ORD042", "ORD041"), "Events sequencing should be consistent.")
  }

  test("Test Data Integrity") {
    val matchingRecords = spark.sql(
      "SELECT COUNT(*) FROM demo.orders WHERE order_id = 'ORD042' AND product_name = 'Bluetooth Keyboard4'"
    ).collect().head.getLong(0)
    assert(matchingRecords == 1, "The data fields should match exactly.")
  }


  def sendMessage(message: String) {
    val topic = "ODS_ORDER_LOG"
    val result = KafkaUtil.send(topic, message);
    println(message)
    println("######")
    KafkaUtil.flush
  }
}
