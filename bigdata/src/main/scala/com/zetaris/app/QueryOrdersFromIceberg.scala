package com.zetaris.app

import com.zetaris.data.storage.SalesReportIcebergTableManager.spark

object QueryOrdersFromIceberg {
  def main(args: Array[String]): Unit = {
    spark.sql("SELECT * FROM demo.orders").show
//    spark.sql("SELECT * FROM demo.orders_temp").show
//    spark.sql("SELECT count(*) FROM demo.orders ").show
    spark.sql("SELECT * FROM demo.orders where order_id = 'ORD021'").show
  }
}
