package eduardo.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    public static void main(String[] args) {

        new ConsumerDemoThreads().run();

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class);

        //subrscribe to topics

        final String topic = "first_topic";
        final String bootstrapServers = "127.0.0.1:9092";

        String groupId = "my-fourth-application";

        CountDownLatch latch = new CountDownLatch(1);


        Runnable consumerRunnable = new ConsumerRunnable(bootstrapServers,
                groupId,
                topic,
                latch);

        Thread myThread = new Thread(consumerRunnable);
        myThread.start();


        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("Application has exited");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("application interrupted", e);
        }finally {
            logger.info("application stopped");
        }
    }

    public class ConsumerRunnable implements Runnable{

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;

        //create consumer properties
        Properties properties = new Properties();
        KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;


            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            //create consumer
            consumer = new KafkaConsumer<>(properties) ;
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {

                        logger.info("key: " + record.key() + ", Value: " + record.value());

                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }
        public void shutdown(){
            consumer.wakeup();
        }
    }

}
