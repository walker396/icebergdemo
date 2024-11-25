package com.zetaris.untils

import com.zetaris.data.bean.StorageConfig
import org.apache.spark.sql.SparkSession

object IcebergUtil {
  def initializeSparkSession(config: StorageConfig): SparkSession = {
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
}
