package com.demo;

import java.util.Properties;

import com.demo.serde.AvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.demo.beans.Stock;
import org.apache.kafka.common.serialization.StringSerializer;


public class AvroProducer {

    public static void main(String[] args) throws Exception {
        Stock[] stocks = new Stock[100];
        for(int i = 0; i<100;i++){
            stocks[i] = new Stock();
            stocks[i].setStockCode(String.valueOf(i));
            stocks[i].setStockName("stock" + i);
            stocks[i].setTradeTime(System.currentTimeMillis());
            stocks[i].setPreClosePrice(100.0F);
            stocks[i].setOpenPrice(88.8F);
            stocks[i].setCurrentPrice(300.0F);
            stocks[i].setLowPrice(12.4F);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());

        Producer<String, Stock> producer = new KafkaProducer<String, Stock>(props);

        for(Stock stock:stocks){
            ProducerRecord<String, Stock> record = new ProducerRecord<String, Stock>("test_topic", stock);
            RecordMetadata metadata = producer.send(record).get();
            StringBuilder sb = new StringBuilder();
            sb.append("stock: ").append(stock.toString()).append(" has been sent successfully!").append("\n")
                    .append("send to partition ").append(metadata.partition())
                    .append(", offset = ").append(metadata.offset());
            System.out.println(sb.toString());
            Thread.sleep(100);

        }

        producer.close();


    }
}
