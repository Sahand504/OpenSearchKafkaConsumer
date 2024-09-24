package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.example.MyOpenSearchClient.OPEN_SEARCH_URL;

public class OpenSearchConsumer {

    static Properties getProperties() {
        Properties properties = new Properties();

        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9093");

        // serializer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "opensearch-consumers");
        properties.setProperty("auto.offset.reset", "latest");

        return properties;
    }

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        logger.info("OpenSearchConsumer started... ");

        MyOpenSearchClient myOpenSearchClient = MyOpenSearchClient.getInstance();
        RestHighLevelClient openSearchClient = myOpenSearchClient.getOpenSearchClient();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(getProperties());
        kafkaConsumer.subscribe(Collections.singleton("wikimedia-recent-changes"));

        try(openSearchClient ; kafkaConsumer) {

            String indexName = "wikimedia";
            myOpenSearchClient.createIndex(indexName);

            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
                logger.info("Received {} records", records.count());

                for(ConsumerRecord<String, String> record: records) {
                    String openSearchId = myOpenSearchClient.storeData(indexName, record.value());
                    logger.info("Document inserted into {}/{}/_doc/{}", OPEN_SEARCH_URL, indexName, openSearchId);
                }
            }

        }

    }

}