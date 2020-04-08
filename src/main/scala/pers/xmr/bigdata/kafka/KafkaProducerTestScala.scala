package pers.xmr.bigdata.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @author xmr
  * @date 2019/4/28 13:35
  * @description kafka生产端测试
  */
object KafkaProducerTestScala {
  def main(args: Array[String]): Unit = {
    val servers = args(0);
    val properties = new Properties()
    // 指定broker的地址清单,地址格式为 host : port
    properties.put("bootstrap.servers", servers)
    // 使用该类将键对象序列化为字节数组
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 使用该类将值对象序列化为字节数组
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 添加消费组(非必须操作,但生产端要和消费端保持一致)
    properties.put("group.id", "g2")
    // 创建生产者对象
    val producer = new KafkaProducer[String, String](properties)
    // 将要发送的消息
    val str = "测试kafka发送端数据发送! ";
    println(str)
    // 创建ProducerRecord对象, 传入参数是主题名, 和要发送的消息内容
    val rcd = new ProducerRecord[String, String]("posp_trade_time", str)
    // 发送消息
    producer.send(rcd)
    // 这里必须要调结束，否则kafka那边收不到消息
    producer.close()

  }
}

