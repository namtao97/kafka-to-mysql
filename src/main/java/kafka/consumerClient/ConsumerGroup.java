package kafka.consumerClient;

import common.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ConsumerGroup {

    private final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class.getName());
    private CountDownLatch latch;
    private List<Consumer> consumers = new ArrayList<>();

    private final String bootstrapServer;
    private final String groupID;
    private final String topic;


    public ConsumerGroup(String bootstrapServer, String groupID, String topic) {
        this.bootstrapServer = bootstrapServer;
        this.groupID = groupID;
        this.topic = topic;

        String storagePrefix = "storage-offset";

        File file = new File(storagePrefix);
        if (!file.exists()) {
            file.mkdirs();
        }
    }


    public void assignListConsumers(List<Consumer> consumers) {
        this.consumers = consumers;

        latch = new CountDownLatch(this.consumers.size());

        for (Consumer consumer : this.consumers) {
            if (!consumer.topic().equals(this.topic) || !consumer.bootstrapServer().equals(this.bootstrapServer) || !consumer.groupID().equals(this.groupID)) {
                throw new IllegalArgumentException("Parameter of Consumer Group and Consumer is different");
            }

            consumer.setLatch(latch);
        }
    }


    public void run() {
        for (Consumer consumer: consumers) {
            Thread thread = new Thread(consumer);
            thread.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");

            for (Consumer consumer : consumers) {
                consumer.shutdown();
            }

            await(this.latch);
            Database.shutdown();

            logger.info("Consumer Group has exited");
        }));

        await(latch);
        Database.shutdown();
    }


    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }
}
