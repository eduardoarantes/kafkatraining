package eduardo.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
        KafkaConsumer<String, String> consumer = createConsumer();

        //subrscribe to topics

        final String topic = "first_topic";
        consumer.subscribe(Collections.singleton(topic));

        //poll data

        while (true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record: records){

                logger.info("key: "+ record.key() + ", Value: "+record.value());

            }
        }

    }

    private static KafkaConsumer<String, String> createConsumer() {
        //create consumer properties
        Properties properties = new Properties();
        final String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String groupId = "my-fourth-application";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //create consumer
        return new KafkaConsumer<>(properties);
    }

}
