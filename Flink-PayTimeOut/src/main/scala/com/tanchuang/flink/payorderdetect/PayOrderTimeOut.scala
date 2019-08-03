package com.tanchuang.flink.payorderdetect

import com.tanchuang.flink.payorderdetect.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 检测用户的订单 创建时间 和支付时间 超过15分钟
 */

object PayOrderTimeOut {
  def main(args: Array[String]): Unit = {

    //    1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取到订单数据
    val orderStream: KeyedStream[OrderEvent, Long] = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    //
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    val orderTimeoutTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderStream, pattern)
    import scala.collection._
    val selectedDstream: DataStream[OrderResult] = patternStream.select(orderTimeoutTag)(
      (patternResultMap: Map[String, Iterable[OrderEvent]], timestamp:Long) => {
        val firstOrderEvent = patternResultMap.getOrElse("begin", Nil).toIterator.next()
        OrderResult(firstOrderEvent.orderId, s"超时：$timestamp")
      }
    )(
      map => {
        val firstEvent: OrderEvent = map.getOrElse("begin", Nil).toIterator.next()

        OrderResult(firstEvent.orderId, "order pay successfully")
      }
    )
    //主输出流的输出
    selectedDstream.print("main stream :  ")

    val orderOutTimeDstream: DataStream[OrderResult] = selectedDstream.getSideOutput(orderTimeoutTag)

    orderOutTimeDstream.print()

    env.execute()
  }

}
