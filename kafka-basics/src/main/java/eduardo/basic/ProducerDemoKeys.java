package eduardo.basic;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        //create producer properties
        Properties properties = new Properties();
        final String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        //send data

        Callback callback = (recordMetadata, e) -> {
            if(e==null){
                logger.info("\n\n" +
                        "Received metadata \n"+
                        "Topic: "+ recordMetadata.topic() +"\n"+
                        "partition: "+ recordMetadata.partition() +"\n"+
                        "offset: "+ recordMetadata.offset() +"\n"+
                        "Timestamp: "+ recordMetadata.timestamp() +"\n"
                );
            }else{
                logger.error("Error while producing", e);
            }
        };
        final String topic = "first_topic";

        for(int i=0; i<10; i++) {

            final String value = "Hello World - " + i;
            final String key = "id_" + i%3;

            logger.info("Sending message with key: "+key);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            producer
                    .send(producerRecord, callback)
                    .get();//this blocks the .send() making it synchronous. Don't use in production
        }

        producer.flush();
        producer.close();

    }

}
