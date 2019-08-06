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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerThread implements Runnable{

    private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private CountDownLatch latch;
    private KafkaConsumer<String, MessageObject> consumer;
    private long minBatchSize;
    private List<ConsumerRecord<String, MessageObject>> buffer;

    private OffsetManager offsetManager;


    public ConsumerThread(String bootstrapServer, String groupID, String topic, CountDownLatch latch, String storagePrefix, long minBatchSize) {
        this.latch = latch;
        this.minBatchSize = minBatchSize;
        this.offsetManager = new OffsetManager(storagePrefix);

        buffer = new ArrayList<>();

        Properties properties = setConsumerProperties(bootstrapServer, groupID);
        this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonDeserializer<>(MessageObject.class));
        this.consumer.subscribe(Collections.singletonList(topic), new MyConsumerRebalanceListener(consumer, storagePrefix));
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

    public void shutdown() {
        this.consumer.wakeup();
    }

    private void insertToDB(List<ConsumerRecord<String, MessageObject>> records) {
        String query = "INSERT INTO `message`(`msg`, `time`) value (?, ?)";

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/kafka?useSSL=false", "root", "namnt");
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            connection.setAutoCommit(false);

            for (ConsumerRecord<String, MessageObject> record : records) {
                preparedStatement.setString(1, record.value().getMessage());
                preparedStatement.setLong(2, record.value().getTime());
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);

        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Batch Update exception", batchUpdateException);
            this.consumer.close();
            this.latch.countDown();
        } catch (SQLException e) {
            logger.error("SQL Exception", e);
            this.consumer.close();
            this.latch.countDown();
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, MessageObject> records = this.consumer.poll(Duration.ofMillis(1000));

                ConsumerRecord<String, MessageObject> lastRecord = null;
                for (ConsumerRecord<String, MessageObject> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    buffer.add(record);
                    lastRecord = record;
                }

                if (buffer.size() >= this.minBatchSize && lastRecord != null) {
                    insertToDB(buffer);
                    offsetManager.saveOffsetInExternalStore(lastRecord.topic(), lastRecord.partition(), lastRecord.offset());
                    buffer.clear();
                }
            }

        } catch (WakeupException e) {
            logger.info("Received shutdown signal");
        } finally {
            this.consumer.close();
            this.latch.countDown();
        }
    }
}
