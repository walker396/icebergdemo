package com.zetaris.config

/**
 * Configuration class
 * */
object Config {
  val KAFKA_BOOTSTRAP_SERVER: String = "kafka.bootstrap-servers"

  val REDIS_HOST: String = "redis.host"
  val REDIS_PORT: String = "redis.port"

  // Spark
  val SPARK_DRIVER_HOST: String = "spark.driver.host"
  val SPARK_SQL_EXTENTIONS: String = "spark.sql.extensions"
  val SPARK_SQL_CATELOG_DEMO: String = "spark.sql.catalog.demo"
  val SPARK_CATELOG_DEMO_TYPE: String = "spark.sql.catalog.demo.type"
  val SPARK_CATELOG_WAREHOUSE: String = "spark.sql.catalog.demo.warehouse"

}
