package org.cardoza.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class ConsumerDemoCooperative {

    // Logger
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {

        String groupId = "my-java-project";
        String topic = "demo_java";

        // Kafka Properties
        Properties properties = new Properties();

        // Set kafka properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJvdWQtZHJha2UtMTMxNzckkdPG3LUL0pZYYymVBBbrp6daf2rMnWQiuwQcaqY\" password=\"NDkxNDI5YjgtYmZmMi00OTkxLTg2YWYtYjA4NWI4N2NiZTYz\";");

        // Set consumer Properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Create current thread reference
        final Thread mainThread = Thread.currentThread();

        // Adding shutdown thread
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            log.info("Detected thread shutdown, let exit by calling consumer.wakeup()...");
            consumer.wakeup();

            // join the main tread , allow the execution of code in main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));


        try {

            // Subscribe a topic
            consumer.subscribe(List.of(topic));

            // Poll data
            while (true) {

                log.info("--------------------------------------- Polling -----------------------------------------------");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

                for (ConsumerRecord<String, String> kafkaRecord : records) {

                    log.info("Key: {} | Value: {}", kafkaRecord.key(), kafkaRecord.value());
                    log.info("Partition: {} | Offset: {}", kafkaRecord.partition(), kafkaRecord.offset());

                }
            }
        } catch (WakeupException e){
            log.info("Consumer shutdown process is started...");
        } catch (Exception e){
            log.error("Unexpected error occurs", e);
        } finally {
            consumer.close();
            log.info("Gracefully shutdown the consumer");
        }

    }
}
