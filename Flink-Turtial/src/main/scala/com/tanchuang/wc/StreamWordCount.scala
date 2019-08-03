package com.tanchuang.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val parameter: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = parameter.get("host")
    val port: Int = parameter.getInt("port")
    val outputPath: String = parameter.get("output")


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream: DataStream[String] = env.socketTextStream(host,port)

    socketStream.flatMap(_.split("\\s")).map((_,1)).keyBy(0).sum(1).writeAsText(outputPath)


    env.execute("Stream Word Count")

  }

}
