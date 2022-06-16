package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class EventConsumer {

    public static void main(String[] args) {

        // create logger for class
        final Logger logger = LoggerFactory.getLogger(EventConsumer.class);
        // create variables for strings
        final String bootstrapServers = "0.0.0.0:9092";
        final String consumerGroupID = "java-group-consumer";
        // create and populate properties object
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);
        // subscribe to topics
        consumer.subscribe(Arrays.asList("Events"));
        // Poll and consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                String[] EventValue = record.value().toString().split(",", 0);
                Event event = new Event(
                        EventValue[0],
                        EventValue[1],
                        EventValue[2],
                        EventValue[3],
                        EventValue[4],
                        EventValue[5],
                        EventValue[6],
                        EventValue[7],
                        EventValue[8]);

                event.Stock("18014", "Spruiten", event, "07/11/2015");

                logger.info("Received new record: \n" +
                        "key: " + record.key() + ", " +
                        "value" + record.value() + ", " +
                        "topic" + record.topic() + ", " +
                        "partition" + record.partition() + ", " +
                        "offset" + record.offset() + "\n");

            }
        }

    }
}
