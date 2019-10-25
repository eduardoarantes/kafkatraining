package eduardo.basic;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        //create producer properties

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);


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

        for(int i=0; i<10; i++) {

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello_world - "+ i);

            producer.send(producerRecord, callback);
        }

        producer.flush();
        producer.close();

    }

}
