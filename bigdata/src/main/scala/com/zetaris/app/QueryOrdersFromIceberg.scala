package com.zetaris.app

import com.zetaris.data.storage.SalesReportIcebergTableManager.spark

object QueryOrdersFromIceberg {
  def main(args: Array[String]): Unit = {
    spark.sql("SELECT * FROM demo.orders").show
  }
}
