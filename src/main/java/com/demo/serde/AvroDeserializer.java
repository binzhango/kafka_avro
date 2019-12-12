package com.demo.serde;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import com.demo.beans.Stock;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroDeserializer implements Deserializer<Stock>{

    public void configure(Map<String, ?> map, boolean b) {

    }

    public Stock deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        Stock stock = new Stock();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DatumReader<Stock> userDatumReader = new SpecificDatumReader<Stock>(stock.getSchema());
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);

        try{
            stock = userDatumReader.read(null,decoder);

        }catch (IOException e){
            e.printStackTrace();
        }

        return stock;
    }

    public void close() {

    }
}
