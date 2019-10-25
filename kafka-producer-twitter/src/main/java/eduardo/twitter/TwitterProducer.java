package eduardo.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    // These secrets should be read from a config file
    final String consumerKey = "3ENWFDHtVhYjrhuhgbdLhyX53";
    final String consumerSecret = "YG5aTOIInwpEhIfAsEgC4EjSdfBLpenfULLGRCxzE2I4vIH9zI";
    final String token = "83362197-yQoAA7FTzD0VSYmiFvhxXa94cQ3Z1H2h9PEDPzYJl";
    final String secret = "VtLvWhiA8juKi7dsKcrloEMN7Lht7EhmzhbByoTA8oGmC";


    List<String> terms = Lists.newArrayList("kafka", "usa", "sport", "java");

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client hosebirdClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();
        KafkaProducer<String, String> producer = createKafkaProducer();


        // add shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("stopping application");
            logger.info("stopping twitter client");
            hosebirdClient.stop();
            logger.info("closing the producer");
            producer.close();
            logger.info("stopping done");
        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if(msg!=null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something wrong", e);
                        }
                    }
                });
            }

        }
    }
    private KafkaProducer<String, String> createKafkaProducer() {
        //create a kafka producer
        Properties properties = new Properties();
        final String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create safer producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


        // high throughtput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));



        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }


    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // create a twitter client


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);


        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        return builder.build();
    }

}
