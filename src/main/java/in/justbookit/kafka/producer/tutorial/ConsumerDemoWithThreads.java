package in.justbookit.kafka.producer.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";
    private static final String GROUP_ID = "my_first_application";


    public ConsumerDemoWithThreads() {

    }


    public static void main(String[] args) {

        new ConsumerDemoWithThreads().run();
    }


    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                BOOTSTRAP_SERVERS,
                TOPIC,
                GROUP_ID,
                countDownLatch
        );

        Thread myThread = new Thread((myConsumerRunnable));
        myThread.start();

        // add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException ex){
                ex.printStackTrace();
            }
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("App interrupted", e);
        } finally {
            logger.info("App is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String boostrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  boostrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while(true){
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record: consumerRecords){
                        logger.info("Key: "+ record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }

                }
            } catch (WakeupException ex){
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown(){
            // it will throw an expception WakeUpException
            consumer.wakeup();
        }
    }
}
