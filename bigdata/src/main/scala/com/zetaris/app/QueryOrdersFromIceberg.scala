package com.zetaris.app

import com.zetaris.data.storage.SalesReportIcebergTableManager.spark

object QueryOrdersFromIceberg {
  def main(args: Array[String]): Unit = {
    spark.sql("SELECT * FROM demo.orders").show
//    spark.sql("SELECT * FROM demo.orders_temp").show
//    spark.sql("SELECT count(*) FROM demo.orders ").show
//    spark.sql("SELECT * FROM demo.orders where order_id = 'ORD021'").show
//    spark.sql("SELECT order_id FROM demo.orders ORDER BY updated_time DESC limit 2").show
    spark.sql("SELECT COUNT(*) as orderCount FROM demo.orders WHERE order_id = 'ORD041' AND product_name = 'Wireless Mouse3'").show
  }
}
