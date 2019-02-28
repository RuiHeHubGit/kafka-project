package com.herui.kafka.spring.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("test")
    private  String topic ;

    @ResponseBody
    @RequestMapping(value = "/producer" , method = RequestMethod.POST)
    public void producer(@RequestParam("message") String msg) {
        kafkaTemplate.send(topic, msg);
    }
}
