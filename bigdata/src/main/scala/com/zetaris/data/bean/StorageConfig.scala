package com.zetaris.data.bean

case class StorageConfig(
                          kafkaConsumer: String,
                          kafkaTopic: String = "ODS_ORDER_LOG",
                          startingOffsets: String = "latest",
                          failOnDataLoss: Boolean = true,
                          maxOffsetsPerTrigger: Int = 3000,
                          warehousePath: String = "src/main/resources/warehouse/catalog/demo/",
                          icebergPath: String = "demo.category_summary",
                          checkpointPath: String = "src/main/resources/warehouse/catalog/demo/checkpoints/",
                          triggerInterval: Long = 10 // seconds
                        )
