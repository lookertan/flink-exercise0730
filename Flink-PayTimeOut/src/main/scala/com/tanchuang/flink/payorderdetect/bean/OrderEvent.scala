package com.tanchuang.flink.payorderdetect.bean

case class OrderEvent( orderId:Long,eventType: String,eventTime:Long)
case class OrderResult(orderId:Long, resultMsg:String)
