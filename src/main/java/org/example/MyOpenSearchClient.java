package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class MyOpenSearchClient {

    private static final Logger logger = LoggerFactory.getLogger(MyOpenSearchClient.class.getSimpleName());

    public static final String OPEN_SEARCH_URL = "https://qa98ufq3pi:oi2yxj5v8n@na-search-7980050836.us-east-1.bonsaisearch.net:443";

    private final RestHighLevelClient openSearchClient;

    private static class MyOpenSearchClientHelper {
        private static final MyOpenSearchClient INSTANCE = new MyOpenSearchClient();
    }

    public static MyOpenSearchClient getInstance() {
        return MyOpenSearchClientHelper.INSTANCE;
    }

    private MyOpenSearchClient() {

        URI uri = URI.create(OPEN_SEARCH_URL);

        String userInfo = uri.getUserInfo();

        if (userInfo == null) {
            openSearchClient = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            openSearchClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
    }

    public RestHighLevelClient getOpenSearchClient() {
        return openSearchClient;
    }

    public void createIndex(String indexName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

        boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

        if(indexExists) {
            logger.info("index already exists...");
        } else {
            logger.info("creating index...");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
    }

    public String storeData(String indexName, String data) throws IOException {
        IndexRequest indexRequest = new IndexRequest(indexName).source(data, XContentType.JSON);
        return openSearchClient.index(indexRequest, RequestOptions.DEFAULT).getId();
    }

    public void addDataToBulk(BulkRequest bulkRequest, String indexName, String data) throws IOException {
        IndexRequest indexRequest = new IndexRequest(indexName).source(data, XContentType.JSON);
        bulkRequest.add(indexRequest);
    }

}
