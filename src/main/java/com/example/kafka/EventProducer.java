package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class EventProducer {

    public static void main(String[] args) throws IOException, URISyntaxException {

        final Logger logger = LoggerFactory.getLogger(EventProducer.class);
        final String CsvFile = "supermarkt.csv";
        // create properties object for producer
        Properties props = new Properties();
        // set bootstrap server
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        // set key serializer
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // set value serializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        URI uri = PriceProducer.class.getClassLoader().getResource(CsvFile).toURI();
        Stream<String> FileStream = Files.lines(Paths.get(uri)).skip(1);

        FileStream.forEach(line -> {
            System.out.println(line);

            ProducerRecord<String, String> Record = new ProducerRecord<String, String>(
                    "Events",
                    "event", line);
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
        });

        // flush and close the producer
        producer.flush();
        producer.close();

    }
}
