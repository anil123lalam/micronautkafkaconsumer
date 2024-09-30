package com.example;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaListener(offsetReset = OffsetReset.EARLIEST,groupId = "first",threads = 5)
public class AnimalListener {
int count =0;
    @Topic("AnimalTopic")// (2)
    public void receive(String name) throws InterruptedException { // (3)
        System.out.println("Thread Name "+Thread.currentThread().getName());
        if(count==0) {
            count++;
        Thread.sleep(500000);
        }
        System.out.println("Received animal "+name+" Thread Name "+Thread.currentThread().getName());
    }
}
