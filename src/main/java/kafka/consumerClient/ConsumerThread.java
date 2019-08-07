package kafka.consumerClient;

import common.Database;
import common.MessageObject;
import kafka.helper.MyConsumerRebalanceListener;
import kafka.helper.OffsetManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class ConsumerThread implements Consumer {

    private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

    private String bootstrapServer;
    private String topic;
    private String groupID;
    private KafkaConsumer<String, MessageObject> consumer;
    private List<ConsumerRecord<String, MessageObject>> buffer;

    private long minBatchSize;
    private CountDownLatch latch;
    private OffsetManager offsetManager;


    public ConsumerThread(String bootstrapServer, String groupID, String topic, OffsetManager offsetManager, long minBatchSize) {
        this.minBatchSize = minBatchSize;
        this.offsetManager = offsetManager;
        this.latch = new CountDownLatch(0); // default is 0 so this consumerThread can run independent
        this.bootstrapServer = bootstrapServer;
        this.groupID = groupID;
        this.topic = topic;

        buffer = new ArrayList<>();

        Properties properties = setConsumerProperties(this.bootstrapServer, this.groupID);
        this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonDeserializer<>(MessageObject.class));
        this.consumer.subscribe(Collections.singletonList(this.topic), new MyConsumerRebalanceListener(this.consumer, this.offsetManager));
    }


    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, MessageObject> records = this.consumer.poll(Duration.ofMillis(1000));

                ConsumerRecord<String, MessageObject> lastRecord = null;
                for (ConsumerRecord<String, MessageObject> record : records) {
                    buffer.add(record);
                    lastRecord = record;
                }

                if (buffer.size() >= this.minBatchSize && lastRecord != null) {
                    Database.insertMessageToDB(buffer);
                    offsetManager.saveOffsetInExternalStore(lastRecord.topic(), lastRecord.partition(), lastRecord.offset());
                    buffer.clear();
                }
            }

        } catch (WakeupException e) {
            logger.info("Received shutdown signal");
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Batch Update exception", batchUpdateException);
        } catch (SQLException e) {
            logger.error("SQL Exception", e);
        } finally {
            this.consumer.close();
            this.latch.countDown();
        }
    }


    @Override
    public String topic() {
        return this.topic;
    }


    @Override
    public String groupID() {
        return this.groupID;
    }


    @Override
    public String bootstrapServer() {
        return this.bootstrapServer;
    }


    // set latch to join a consumer group and run together
    @Override
    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }


    public void shutdown() {
        this.consumer.wakeup();
    }


    private Properties setConsumerProperties(String bootstrapServer, String groupID) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }
}
