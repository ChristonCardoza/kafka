package org.cardoza.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class ConsumerDemo {

    // Logger
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        String groupId = "my-java-project";
        String topic = "demo_java";

        // Kafka Properties
        Properties properties = new Properties();

        // Set kafka properties
        properties.setProperty("bootstrap.servers", "proud-drake-13177-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJvdWQtZHJha2UtMTMxNzckkdPG3LUL0pZYYymVBBbrp6daf2rMnWQiuwQcaqY\" password=\"NDkxNDI5YjgtYmZmMi00OTkxLTg2YWYtYjA4NWI4N2NiZTYz\";");

        // Set consumer Properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // Create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // Subscribe a topic
        kafkaConsumer.subscribe(List.of(topic));

        // Poll data
        while (true){
            log.info("--------------------------------------- Polling -----------------------------------------------");

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));

            for(ConsumerRecord<String, String> record : records){

                log.info("Key: {} | Value: {}", record.key(), record.value());
                log.info("Partition: {} | Offset: {}", record.partition(), record.offset());

            }
        }


    }
}
