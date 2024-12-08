package org.cardoza.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    // Logger
    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        // Kafka Properties
        Properties properties = new Properties();

        // Set kafka properties
        properties.setProperty("bootstrap.servers", "proud-drake-13177-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJvdWQtZHJha2UtMTMxNzckkdPG3LUL0pZYYymVBBbrp6daf2rMnWQiuwQcaqY\" password=\"NDkxNDI5YjgtYmZmMi00OTkxLTg2YWYtYjA4NWI4N2NiZTYz\";");

        // Set producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello-kafka");

        // Send data
        producer.send(producerRecord);

        // flush -> block it until all sending complete
        producer.flush();

        // Close the producer
        producer.close();
    }
}
