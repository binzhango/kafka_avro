package com.demo.serde;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.demo.beans.Stock;

public class AvroSerializer implements Serializer<Stock> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, Stock stock) {
        if(stock == null){
            return null;
        }
        DatumWriter<Stock> writer = new SpecificDatumWriter<Stock>(stock.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try{
            writer.write(stock, encoder);
        }catch (IOException e){
            throw new SerializationException(e.getMessage());
        }
        return out.toByteArray();
    }

    public void close() {

    }
}
