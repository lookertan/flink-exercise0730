package com.tanchuang.flink.hello

import java.{lang, util}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 1.检测用户登陆
 * 2.如果2秒内，有客户连续两次登陆失败则报警
 */
case class LoginEvent( userId:Long,ip:String,eventType:String,eventTime:Long )
//主要产生报警的信息
case class WarnMessage( userId:Long,firstTime:Long,lastFailTime:Long,warningMsg:String)
object LoginFailDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)  //设置时间特性 以事件为主
    env.setParallelism(1)


    val loginStream: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.0.1", "fail", 1558430846),
      LoginEvent(2, "192.168.0.1", "fail", 1558430847),
      LoginEvent(2, "192.168.0.1", "fail", 1558430848),
      LoginEvent(2, "192.168.0.1", "fail", 1558430859),
      LoginEvent(2, "192.168.0.1", "fail", 1558430860)
    ))
    loginStream.assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)
      .process( new MatchFunction())
      .print()

    env.execute()

  }
}

class MatchFunction() extends KeyedProcessFunction[Long,LoginEvent,WarnMessage]{
  import scala.collection.JavaConversions._
  lazy val loginState :ListState[LoginEvent] =
    getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, WarnMessage]#Context, out: Collector[WarnMessage]): Unit = {
    if (value.eventType == "fail"){
      val value1: util.Iterator[LoginEvent] = loginState.get().iterator()
      if (value1.hasNext){
        val event: LoginEvent = value1.next()
        val firstTime: Long = event.eventTime
        val secondTime: Long = value.eventTime

        if (secondTime < firstTime +2) {
          //如果是两秒内发生的情况
          out.collect(WarnMessage(value.userId, firstTime, secondTime, "login fail in 2 seconds"))
        }
          //如果不是两秒内发生的情况   成不成功都要更新状态
          loginState.update(value::Nil)

      }else{
        //如果是第一次登陆的情况: 将这次的状态放入
        loginState.add(value)
      }
    }else{
      //如果状态是登陆成功的情况
      loginState.clear()
    }
  }
}
