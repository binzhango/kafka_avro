package com.demo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import com.demo.serde.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.demo.beans.Stock;
import org.apache.kafka.common.serialization.StringDeserializer;



public class AvroConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_topic_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());

        KafkaConsumer<String, Stock> consumer = new KafkaConsumer<String, Stock>(props);
        consumer.subscribe(Arrays.asList("test_topic"));

        try{
            while(true){
                ConsumerRecords<String, Stock> records = consumer.poll(100);
                for(ConsumerRecord<String, Stock> record:records){
                    Stock stock = record.value();
                    System.out.println(stock.get("stockCode"));
                }
            }
        }finally {
            consumer.close();
        }
    }
}
