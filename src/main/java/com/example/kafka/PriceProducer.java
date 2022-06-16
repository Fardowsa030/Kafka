package com.example.kafka;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriceProducer {

    public static void main(String[] args) throws IOException, URISyntaxException {
        final Logger logger = LoggerFactory.getLogger(PriceProducer.class);
        // create properties object for producer
        Properties prop = new Properties();
        // set bootstrap server
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        // set key serializer
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // set value serializer
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        ProducerRecord<String, String> Record = new ProducerRecord<String, String>(
                "prices",
                "price", "1.50");

        producer.send(Record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // this method is gonna be called if our message is succesfully wrtitten in the kafka topic
                        if (e == null) {
                            logger.info("\n Received record metadata:\n" +
                                    "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", " +
                                    "Offset: " + recordMetadata.offset() + "@ Timestamp: " + recordMetadata.timestamp() + "\n");

                        } else {
                            logger.error("Error Occurred", e);
                        }
                    }
                });

        // flush and close the producer
        producer.flush();
        producer.close();

    }

    }




