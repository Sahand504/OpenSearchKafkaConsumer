package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


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

            // get reference to the main thread
            final Thread mainTread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    logger.info("Shutdown detected...");

                    logger.info("calling wakeup to throw wakeup exception...");
                    kafkaConsumer.wakeup();

                    // join the main thread to allow execution of the code
                    try {
                        mainTread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
                logger.info("Received {} records!", records.count());

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record: records) {
//                    String openSearchId = myOpenSearchClient.storeData(indexName, record.value());
//                    logger.info("Document inserted into {}/{}/_doc/{}", OPEN_SEARCH_URL, indexName, openSearchId);
                    myOpenSearchClient.addDataToBulk(bulkRequest, indexName, record.value());
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("inserted {} bulk records!", bulkResponse.getItems().length);
                    logger.info("Commited {} records!", records.count());

                    // Just to increase the chance to have more data in the bulk
                    // testing purpose
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }

                kafkaConsumer.commitSync();

            }

        } catch (WakeupException wakeupException) {
            logger.info("Consumer is starting to shutdown!");
        } catch (Exception e) {
            logger.error("Unexpected exception!", e);
        } finally {
            kafkaConsumer.close();
            openSearchClient.close();
            logger.info("Consumer is now gracefully shutdown!");
            logger.info("open-search client is closed!");
        }

    }

}