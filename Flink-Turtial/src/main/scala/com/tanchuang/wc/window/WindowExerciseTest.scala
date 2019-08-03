package com.tanchuang.wc.window

import com.tanchuang.wc.sink.{MySourceFunction, Sensor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WindowExerciseTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val mySource: DataStream[String] = env.socketTextStream("hadoop102",9999)

    mySource.map(msg => {
      val Array(id, timestamp, temprature): Array[String] = msg.split(",")
      Sensor(id.trim, timestamp.trim.toLong, temprature.trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.milliseconds(1000L)) {
        override def extractTimestamp(element: Sensor): Long = {
          element.timeStamp*1000L
        }
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((s1,s2)=> if (s1.temprature >s2.temprature) s2 else s1)
      .print("Window: ")


      env.execute("WIndow Exercise")
  }

}
