package com.zjx.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SbKafkaDemoController {

    public static final String TOPIC = "kafka_demo";
    @Autowired
    KafkaTemplate kafkaTemplate;

    /**
     * 生产消息
     * @param input
     * @return
     */
    @GetMapping("/send/{input}")
    public String sendToKafka(@PathVariable("input") final String input){
//        kafkaTemplate.send(TOPIC, input);

        //事务的支持
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback() {
            @Override
            public Object doInOperations(KafkaOperations kafkaOperations) {
                kafkaOperations.send(TOPIC, input);
                if("error".equals(input)){
                    throw new RuntimeException("inpput is error");
                }
                kafkaOperations.send(TOPIC, input + " author");
                return true;
            }
        });
        return "send success:" + input;
    }

    //支持事务
    @GetMapping("/send2/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public String sendToTran(@PathVariable("input") final String input){
        //事务的支持
        kafkaTemplate.send(TOPIC, input);
        if("error".equals(input)){
            throw new RuntimeException("inpput is error");
        }
        kafkaTemplate.send(TOPIC, input + " author");
        return "send success:" + input;
    }

    /**
     * 消费消息
     * @param input
     */
    @KafkaListener(id = "", topics = TOPIC, groupId = "group.demo")
    public void listener(String input){
        System.out.println("监听到的消息:" + input);
    }

}
