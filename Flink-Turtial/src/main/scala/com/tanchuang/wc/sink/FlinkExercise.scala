package com.tanchuang.wc.sink

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object FlinkExercise {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),props))
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)


    val splitedStream: SplitStream[Int] = stream.split(x => {
      if (x % 2 == 0) Seq("偶数")
      else Seq("奇数")
    })
    val oddStream: DataStream[Int] = splitedStream.select("奇数")
    val evenStream: DataStream[Int] = splitedStream.select("偶数")
    val allStream: DataStream[Int] = splitedStream.select("奇数", "偶数")
    val connectedStream: ConnectedStreams[Int, Int] = oddStream.connect(evenStream)

    val mapStream: DataStream[String] = connectedStream.map(odd => "奇数" + odd, even => "偶数：" + even)


    oddStream.union(evenStream).print()


//    mapStream.print()


    env.execute()

  }

}
