package com.bjsxt.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaSource1 {

  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //连接Kafka，并且Kafka中的数据是普通字符串（String）
    val props = new Properties()
    //5个基本属性
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","fink01")//消费者
    //因为kafka里的数据是流形式，要转化成字符串，则应该反序列化。键和值都要反序列化。
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")

    val stream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("t_bjsxt",new SimpleStringSchema(),props))

    stream.print()


    streamEnv.execute()
  }
}
