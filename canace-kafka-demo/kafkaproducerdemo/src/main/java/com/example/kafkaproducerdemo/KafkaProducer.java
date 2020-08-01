package com.example.kafkaproducerdemo;

import com.pamirs.pradar.Pradar;
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducer {

    public static Producer<String, String> producer;
    public final static String TOPIC = "canace";
    public static Logger logger= LoggerFactory.getLogger(KafkaProducer.class);

    public static Producer<String, String> KafkaProducer() {
        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "192.168.100.35:9092");

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks", "-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
        return producer;
    }

    public static void produce(Producer<String, String> producer) {
        int messageNo = 0;
        final int COUNT = 5;
//        Pradar.setClusterTest(true);
//        logger.info(">>>>>>>>>>>"+Pradar.isClusterTest()+">>>>>>>>>");
        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "test2222>>>>>>>" + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
            System.out.println(TOPIC+""+data);
            messageNo++;
        }
    }
    public static void main(String[] args){
        Producer<String, String> producer= KafkaProducer();
        produce(producer);
    }
}