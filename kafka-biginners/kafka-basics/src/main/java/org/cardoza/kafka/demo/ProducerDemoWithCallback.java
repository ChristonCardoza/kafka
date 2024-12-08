package org.cardoza.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {

    // Logger
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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
        properties.setProperty("batch.size", "2");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<30; i++) {
            // Create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello-kafka--" + i);

            // Send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // call every time when record is sent or exception occurs

                    if (exception == null) {

                        // record was sent successfully
                        log.info("Received new metadata \n " +
                                        "Topic: {} \n " +
                                        "partition: {} \n " +
                                        "offset: {} \n " +
                                        "TimeStamp: {} \n ",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {

                        log.error("Exception occur while sending the data:", exception);
                    }

                    log.info("---------------------------------------------------------------------------------------------------------");

                }
            });
        }

        // flush -> block it until all sending complete
        producer.flush();

        // Close the producer
        producer.close();
    }
}
