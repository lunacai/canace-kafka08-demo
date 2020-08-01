package com.example.kafkaproducerdemo.Controller;

import com.example.kafkaproducerdemo.KafkaProducer;
import com.pamirs.pradar.Pradar;
import kafka.javaapi.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/demo")
public class ProducerController {
    public static Logger logger= LoggerFactory.getLogger(KafkaProducer.class);
    public KafkaProducer kafkapro =new KafkaProducer();

    @GetMapping("/producer")
    public String updProducer(){
        logger.info(">>>>>>>>>>>11111>>>>>>>>>");
        Producer<String, String> producer=kafkapro.KafkaProducer();
        Pradar.setClusterTest(true);
        logger.info(">>>>>>>>>>>"+Pradar.isClusterTest()+">>>>>>>>>");
        kafkapro.produce(producer);
        logger.info(">>>>>>>>>>>11111>>>>>>>>>");
        return "sucess";
    }
}
