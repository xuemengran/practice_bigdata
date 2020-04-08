package pers.xmr.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author xmr
 * @date 2019/4/28 13:41
 * @description Kafka生产端(Java版)
 */
public class KafkaProducerTestJava {
    public static void main(String[] args) {
        Properties properties = new Properties();
        String servers = args[0];
        // 指定broker的地址清单,地址格式为 host : port
        properties.put("bootstrap.servers", servers);
        // 使用该类将键对象序列化为字节数组
        properties.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        // 使用该类将值对象序列化为字节数组
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 添加消费组(非必须操作,但生产端要和消费端保持一致)
        properties.put("group.id", "g2");
        // 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        String str = "Kafka推送端测试! ";
        System.out.println(str);
        // 创建ProducerRecord对象, 传入参数是主题名, 和要发送的消息内容
        ProducerRecord producerRecord = new ProducerRecord<String, String>("test", str);
        // 发送消息 (消息先被放进缓冲区,然后使用单独的线程发送到服务器端)
        kafkaProducer.send(producerRecord);
        // 关闭
        kafkaProducer.close();
    }
}


