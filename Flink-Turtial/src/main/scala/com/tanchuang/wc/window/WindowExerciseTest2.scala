package com.tanchuang.wc.window

import com.tanchuang.wc.sink.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowExerciseTest2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(3000)
    env.setParallelism(1)


     val stream: DataStream[String] = env.socketTextStream("hadoop102",9999)

    stream.filter(msg => !msg.isEmpty).map(msg => {
      val Array(id,timestamp,temprature): Array[String] = msg.split(",")
      Sensor(id.trim,timestamp.trim.toLong,temprature.trim.toDouble)
    }).assignTimestampsAndWatermarks(new MyAssigner2())
      .keyBy(_.id)
      .timeWindow(Time.seconds(15),Time.seconds(5))
      .reduce((s1,s2) => if (s1.temprature > s2.temprature) s2 else s1)
      .print()

    env.execute()


  }

}

class MyAssigner2() extends AssignerWithPunctuatedWatermarks[Sensor]{

  override def checkAndGetNextWatermark(lastElement: Sensor, extractedTimestamp: Long): Watermark = {
    if ( lastElement.id == "sensor_10"){
      new Watermark(extractedTimestamp)
    }else{
      null
    }
  }

  override def extractTimestamp(element: Sensor, previousElementTimestamp: Long): Long = {
    element.timeStamp*1000
  }
}


class MyAssigner() extends AssignerWithPeriodicWatermarks[Sensor]{
  private val bound:Long = 1000L
  private var maxTs:Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    println(System.currentTimeMillis())
    new Watermark(maxTs - bound)
//    new Watermark(bound)
  }

  override def extractTimestamp(element: Sensor, previousElementTimestamp: Long): Long = {
    maxTs =  (element.timeStamp*1000).max(maxTs)
    element.timeStamp*1000L
  }
}
