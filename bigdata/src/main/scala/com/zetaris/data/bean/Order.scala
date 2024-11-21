package com.zetaris.data.bean
@SerialVersionUID(123456789L)
case class Order(
                  order_id: String,
                  user_id: String,
                  product_id: String,
                  product_name: String,
                  category: String,
                  price: Double,
                  quantity: Int,
                  order_date: String,
                  order_status: String,
                  delivery_date: String
                ) extends Serializable
