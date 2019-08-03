package com.tanchuang.flink.hello


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用cep 处理复杂事件情况
 */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "success", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.0.1", "success", 1558430846),
      LoginEvent(2, "192.168.0.1", "fail", 1558430847),
      LoginEvent(2, "192.168.0.1", "fail", 1558430848),
      LoginEvent(2, "192.168.0.1", "fail", 1558430859),
      LoginEvent(2, "192.168.0.1", "fail", 1558430861)
    )
    ).assignAscendingTimestamps(_.eventTime * 1000)

    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))


    val patternStream: PatternStream[LoginEvent] = CEP.pattern(source, pattern)
    import scala.collection.Map

    val warnMessage: DataStream[WarnMessage] = patternStream.select((partternResult: Map[String, Iterable[LoginEvent]]) => {
      val firstFailEvent: LoginEvent = partternResult.getOrElse("begin", Nil).toIterator.next()
      val secondFailEvent: LoginEvent = partternResult.getOrElse("next", Nil).toIterator.next()
      WarnMessage(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "fail login Warining ")
    })
    warnMessage.print()

    env.execute("Login fail time detect")


  }

}
