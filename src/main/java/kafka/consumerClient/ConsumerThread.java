package kafka.consumerClient;

import common.Database;
import common.MessageObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
    private List<ConsumerRecord<String, MessageObject>> buffer = new ArrayList<>();

    private long minBatchSize;
    private CountDownLatch latch;
    private Database database;


    public ConsumerThread(Properties properties, Database database) {
        this.latch = new CountDownLatch(0); // default is 0 so this consumerThread can run independent
        this.database = database;

        this.bootstrapServer = properties.getProperty("bootstrap.servers");
        this.groupID = properties.getProperty("group.id");
        this.topic = properties.getProperty("topic");
        this.minBatchSize = Long.parseLong(properties.getProperty("min.batch.size"));

        this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonDeserializer<>(MessageObject.class));
        this.consumer.subscribe(Collections.singletonList(this.topic), new MyConsumerRebalanceListener(this, this.database));
    }


    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, MessageObject> records = this.consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, MessageObject> record : records) {
                    buffer.add(record);
                }

                if (buffer.size() >= this.minBatchSize) {
                    database.insertMessageToDB(buffer);
                    buffer.clear();
                }
            }

        } catch (WakeupException e) {
            logger.info("Received shutdown signal");
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Batch Update exception", batchUpdateException);
        } catch (Exception e) {
            logger.error("Database Exception", e);
            e.printStackTrace();
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


    @Override
    public long position(TopicPartition partition) {
        return this.consumer.position(partition);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        this.consumer.seek(partition, offset);
    }


    public void shutdown() {
        this.consumer.wakeup();
    }
}
