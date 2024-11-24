package com.zetaris.app

import com.zetaris.config.Config
import com.zetaris.untils.PropertiesUtil
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object IntegrationService {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val config = parseConfig()
  var spark: SparkSession = initializeSparkSession(config)
  case class StorageConfig(
                            kafkaConsumer: String = PropertiesUtil(Config.KAFKA_BOOTSTRAP_SERVER),
                            kafkaTopic: String = "ODS_ORDER_LOG",
                            startingOffsets: String = "latest",
                            failOnDataLoss: Boolean = true,
                            maxOffsetsPerTrigger: Int = 3000,
                            warehousePath: String = "src/main/resources/warehouse/catalog/demo/",
                            icebergPath: String = "demo.category_summary",
                            checkpointPath: String = "src/main/resources/warehouse/catalog/demo/checkpoints/",
                            triggerInterval: Long = 10 // seconds
                          )

  private val orderSchema = StructType(List(
    StructField("order_id", StringType), // Unique identifier for the order
    StructField("user_id", StringType), // Unique identifier for the user
    StructField("product_id", StringType), // Unique identifier for the product
    StructField("product_name", StringType), // Name of the product
    StructField("category", StringType), // Category of the product
    StructField("price", DoubleType), // Price of a single unit
    StructField("quantity", IntegerType), // Quantity of the product ordered
    StructField("order_date", DateType), // Date when the order was placed
    StructField("order_status", StringType), // Current status of the order
    StructField("delivery_date", DateType) //, // Date when the order was delivered
  ))

  def main(args: Array[String]): Unit = {
//    val config = parseConfig()

    Try {

      ensureStorageTableExists(spark)
      ensureTempTableExists(spark)
      val streamingQuery = processOrderSummaryStream(spark, config)

      setupShutdownHook(streamingQuery, spark)
//      if(args != null && args.nonEmpty && args(0) == "test"){
//        streamingQuery.awaitTermination(60000L)
//      }else{
        streamingQuery.awaitTermination()
//      }

    } match {
      case Success(_) => logger.info("Order summary storage processing completed successfully")
      case Failure(e) =>
        logger.error("Order storage processing failed", e)
        System.exit(1)
    }
  }

  def parseConfig(): StorageConfig = {
    StorageConfig()
  }

  def initializeSparkSession(config: StorageConfig): SparkSession = {
    logger
    SparkSession.builder()
      .master("local[2]")
      .appName("OrderStaticsStorage")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.demo.type", "hadoop")
      .config("spark.sql.catalog.demo.warehouse", config.warehousePath)
      .config("spark.sql.streaming.checkpointLocation", config.checkpointPath)
      .config("spark.sql.streaming.minBatchesToRetain", "100")
      .config("spark.sql.streaming.pollingDelay", "1000")
      .config("spark.sql.streaming.maxBatchDuration", "10 seconds")
      .getOrCreate()
  }

  def ensureStorageTableExists(spark: SparkSession): Unit = {
    logger.info("Ensuring order storage table exists...")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS demo.orders (
        |    order_id STRING COMMENT 'Unique identifier for the order',
        |    user_id STRING COMMENT 'Unique identifier for the user',
        |    product_id STRING COMMENT 'Unique identifier for the product',
        |    product_name STRING COMMENT 'Name of the product',
        |    category STRING COMMENT 'Category of the product',
        |    price DOUBLE COMMENT 'Price of a single unit',
        |    quantity INT COMMENT 'Quantity of the product ordered',
        |    order_date DATE COMMENT 'Date when the order was placed',
        |    order_status STRING COMMENT 'Current status of the order',
        |    delivery_date DATE COMMENT 'Date when the order was delivered',
        |    updated_time TIMESTAMP COMMENT 'Date when the order was updated'
        |)
        |USING iceberg
        |PARTITIONED BY (category);
        |""".stripMargin)
  }

  def ensureTempTableExists(spark: SparkSession): Unit = {
    logger.info("Ensuring temp order storage table exists...")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS demo.orders_temp (
        |    order_id STRING COMMENT 'Unique identifier for the order',
        |    user_id STRING COMMENT 'Unique identifier for the user',
        |    product_id STRING COMMENT 'Unique identifier for the product',
        |    product_name STRING COMMENT 'Name of the product',
        |    category STRING COMMENT 'Category of the product',
        |    price DOUBLE COMMENT 'Price of a single unit',
        |    quantity INT COMMENT 'Quantity of the product ordered',
        |    order_date DATE COMMENT 'Date when the order was placed',
        |    order_status STRING COMMENT 'Current status of the order',
        |    delivery_date DATE COMMENT 'Date when the order was delivered'
        |)
        |USING iceberg
        |PARTITIONED BY (category);
        |""".stripMargin)
  }

  def processOrderSummaryStream(spark: SparkSession, config: StorageConfig): StreamingQuery = {
    logger.info(s"Starting order summary stream processing from topic: ${config.kafkaTopic}")
    val kafkaStream = readOrderSummaryFromKafka(spark, config)
    val transformedStream = transformOrderSummary(kafkaStream)
    writeOrderSummaryToIceberg(transformedStream, config)
  }

  private def readOrderSummaryFromKafka(spark: SparkSession, config: StorageConfig): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaConsumer)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", config.startingOffsets)
      .option("failOnDataLoss", config.failOnDataLoss)
      .option("maxOffsetsPerTrigger", config.maxOffsetsPerTrigger)
      .option("kafka.max.poll.records", "3000")
      .option("kafka.session.timeout.ms", "30000")
      .option("kafka.request.timeout.ms", "40000")
      .load()
  }

  private def transformOrderSummary(df: DataFrame): DataFrame = {
    df.selectExpr("CAST(value AS STRING) as json_value")
      .withColumn("data", from_json(col("json_value"), orderSchema))
      .select("data.*")

  }

  def mergeToIceberg(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
    //create a temporary table to store data
    //    batchDF.createTempView("demo.order_updates")
    batchDF.write
      .format("iceberg")
//      .option("path", "demo.orders_temp")
      .mode("overwrite")
      .saveAsTable("demo.orders_temp")

//    println(s"Batch ID: $batchId, Record Count: ${batchDF.count()}")
    batchDF.sparkSession.sql("REFRESH TABLE demo.orders_temp")
    batchDF.sparkSession.sql("REFRESH TABLE demo.orders")
    //     execute the merge into operation
    batchDF.sparkSession.sql(
      """
      MERGE INTO demo.orders AS target
      USING (select * from demo.orders_temp) AS source
      ON target.order_id = source.order_id
      WHEN MATCHED THEN
        UPDATE SET target.order_status = source.order_status,
                   target.delivery_date = source.delivery_date,
                   target.updated_time = CURRENT_TIMESTAMP
      WHEN NOT MATCHED THEN
      INSERT (order_id, user_id,product_id,product_name,category,price,quantity,order_date,order_status,delivery_date,updated_time)
        VALUES (source.order_id, source.user_id,source.product_id,
        source.product_name,source.category,source.price,source.quantity,
        source.order_date,source.order_status,source.delivery_date, CURRENT_TIMESTAMP);
    """)
  }


  private def writeOrderSummaryToIceberg(df: DataFrame, config: StorageConfig): StreamingQuery = {
    // create a temporary view to store input data
    val query = df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//        // 打印每批次的数据，查看内容
//        println("````````````````````")
//        batchDF.show(false) // 只打印批次内容，不截断
//        println("````````````````````")
        // 调用你的 mergeToIceberg 逻辑进行数据存储
        mergeToIceberg(batchDF, batchId)
      }
      .outputMode("append")
//      .trigger(Trigger.ProcessingTime("3 seconds"))
      .option("checkpointLocation", config.checkpointPath)
      .start()
    query
  }

  private def setupShutdownHook(query: StreamingQuery, spark: SparkSession): Unit = {
    sys.addShutdownHook {
      logger.info("Gracefully shutting down order summary storage...")
      query.stop()
      spark.stop()
    }
  }
}

// Monitor and metrics specification
object OrderSummaryMetrics {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def recordProcessingMetrics(batchId: Long, numRecords: Long, processingTime: Long): Unit = {
    logger.info(s"Batch $batchId: Processed $numRecords order summaries in $processingTime ms")
  }
}
