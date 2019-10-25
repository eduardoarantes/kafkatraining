package kafkatutorial;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");


        while (true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));


            final int count = records.count();
            logger.info("Received records: "+ count);


            if(count==0){
                continue;
            }

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record: records){

                String jsonString = record.value();

                //String id = record.value() + "_" + record.partition() + "_" + record.offset();
                String id;
                try {
                    id = extractIdFromTweets(record.value());
                }catch (NullPointerException e){
                    logger.error("Error reading tweet id");
                    id = record.value() + "_" + record.partition() + "_" + record.offset();
                }

                IndexRequest indexRequest = createIndexRequest(client, jsonString, id);

                bulkRequest.add(indexRequest);
            }

            BulkResponse bulkItemResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

            logger.info("committing offsets");
            consumer.commitSync();
            logger.info("offsets committed");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //client.close();

    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweets(String value) {
        return jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
    }

    private static IndexRequest createIndexRequest(RestHighLevelClient client, String jsonString, String id) throws IOException {

        // kafka eneric

        IndexRequest indexRequest = new IndexRequest(
                "twitter"
        ).id(id).source(jsonString, XContentType.JSON);

        logger.info(id);

        return indexRequest;
    }

    private static RestHighLevelClient createClient() {
        String hostname = "kafka-tutorial-167269388.ap-southeast-2.bonsaisearch.net";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        String username = "8i8qg7rvp9";
        String password = "xajlbdzelb";
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));


        RestClientBuilder clientBuilder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(
                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                );

        RestHighLevelClient client = new RestHighLevelClient(clientBuilder);

        return client;
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {
        //create consumer properties
        Properties properties = new Properties();
        final String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        String groupId = "kafka-demo-elasticsearch1";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        //create consumer
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        return kafkaConsumer;
    }

}
