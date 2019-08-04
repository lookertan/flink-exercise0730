package com.tanchuang.wc.state.com.tanchuang.state.util

import java.io.{BufferedInputStream, BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties


object ConfigurationUtil {

  private val props = new Properties()
  props.load(getReader("D:\\workspace\\flink-exercise\\Flink-Turtial\\src\\main\\resources\\config.properties"))

  def getReader(file: String): BufferedReader = {
    val fileInputStream = new FileInputStream(file)
    val inputReader = new InputStreamReader(fileInputStream)
    val reader = new BufferedReader(inputReader)
    reader
  }


  def getPropery(key: String): String = {
    props.getProperty(key)
  }
}
