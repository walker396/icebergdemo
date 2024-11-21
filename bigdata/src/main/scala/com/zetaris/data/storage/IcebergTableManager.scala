package com.zetaris.data.storage

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession}

object SalesReportIcebergTableManager extends Logging with Serializable {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    // Iceberg specific configs
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", "src/main/resources/warehouse/catalog/demo/")
    .getOrCreate()

}
