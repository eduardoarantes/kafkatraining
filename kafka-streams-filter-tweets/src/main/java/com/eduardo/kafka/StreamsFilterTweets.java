package com.eduardo.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    static Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class);

    public static void main(String[] args) {
        // create properties

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        //create topology

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersFromTweets(jsonTweet)>10000
        );

        filteredStream.to("important_tweets");
        //build topology

        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(), properties
        );


        //start our streams application
        kafkaStreams.start();

    }

    private static JsonParser jsonParser = new JsonParser();
    private static Integer extractUserFollowersFromTweets(String value) {
        try {
            return jsonParser.parse(value)
                    .getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e){
            logger.error("NullPointerException when getting followers", e);
            return 0;
        }
    }


}
