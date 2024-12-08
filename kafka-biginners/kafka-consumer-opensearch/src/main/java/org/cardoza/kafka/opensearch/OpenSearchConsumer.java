package org.cardoza.kafka.opensearch;

import com.google.gson.Gson;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.core5.http.HttpHost;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.OpenSearchTransport;


import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.security.KeyManagementException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws KeyManagementException {

        // Create Opensearch Client
        OpenSearchClient openSearchClient = createOpenSearchClient();

        // create our kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Map<String, Objects> document = null;

        // Create a index in opensearch if it is not exist
        try ( consumer) {
            String index = "wikimedia";
            ExistsRequest existsRequest = new ExistsRequest.Builder().index(index).build();
            boolean isIndexExists = openSearchClient.indices().exists(existsRequest).value();

            if (!isIndexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(index).build();
                openSearchClient.indices().create(createIndexRequest);
                log.info("Wikimedia Index has been created");
            } else {
                log.info("Wikimedia Index already exists!");
            }

            // we subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentChange"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records(s)");

//                BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

                for (ConsumerRecord<String, String> record : records) {

                    // send the record into opensearch
                    document = gson.fromJson(record.value(), Map.class);

                    System.out.println(gson.toJson(document));


//                    IndexRequest indexRequest = new IndexRequest.Builder<>()
//                            .index(index)
//                            .document(document)
//                            .build();

//
//                    IndexResponse response = openSearchClient.index(indexRequest);

//                    Map<String, Objects> finalDocument = document;
//                    bulkRequest.operations(op -> op
//                            .index( idx -> idx
//                                    .index(index)
//                                    .document(finalDocument)));
//
//                    log.info(response.id());
                }
//                openSearchClient.bulk(bulkRequest.build());

                consumer.commitAsync();
                System.out.println("Offset are commited!");
            }
        } catch (Exception e) {
             System.out.println(e);

        } finally {

            consumer.close();
            openSearchClient.shutdown();
            log.info("The consumer is now gracefully shut down");
        }

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch";

        // Kafka Properties
        Properties properties = new Properties();

        // Set kafka properties
        properties.setProperty("bootstrap.servers", "192.168.29.70:9092");

        // Set consumer Properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");

        // Create consumer
        return new KafkaConsumer<>(properties);
    }

    public static OpenSearchClient createOpenSearchClient() throws KeyManagementException {

        String connStr = "https://5kieg83ro1:gdctvzkgoj@kafka-1795455941.us-east-1.bonsaisearch.net:443";
        URI connUri = URI.create(connStr);
        String userInfo = connUri.getUserInfo();

        // REST client with security
        String[] auth = userInfo.split(":");
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                new AuthScope(connUri.getHost(), connUri.getPort()),
                new UsernamePasswordCredentials(auth[0], auth[1].toCharArray())
        );


        HttpHost host = new HttpHost(connUri.getScheme().toLowerCase(), connUri.getHost(), connUri.getPort());

        // Create transport
        OpenSearchTransport transport =  ApacheHttpClient5TransportBuilder.builder(host)
                        .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                )

        .build();

        // Return OpenSearch client
        return new OpenSearchClient(transport);

    }
}
