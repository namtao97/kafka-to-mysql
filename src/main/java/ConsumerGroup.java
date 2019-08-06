import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ConsumerGroup {
    private final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class.getName());
    private CountDownLatch latch;
    private List<ConsumerThread> consumers;

    public ConsumerGroup(String bootstrapServer, String groupID, String topic, int numberOfThread, long minBatchSize) {
        if (numberOfThread < 1) throw new IllegalArgumentException();

        String storagePrefix = "storage-offset";

        File file = new File(storagePrefix);
        if (!file.exists()) {
            file.mkdirs();
        }

        latch = new CountDownLatch(numberOfThread);
        consumers = new ArrayList<>();
        for (int i = 0; i < numberOfThread; i++) {
            ConsumerThread consumerThread = new ConsumerThread(bootstrapServer, groupID, topic, latch, storagePrefix, minBatchSize);
            consumers.add(consumerThread);
        }
    }

    public void run() {
        for (ConsumerThread consumerThread : consumers) {
            Thread thread = new Thread(consumerThread);
            thread.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");

            for (ConsumerThread consumer : consumers) {
                consumer.shutdown();
            }
            await(this.latch);

            logger.info("Consumer Group has exited");
        }));

        await(latch);
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
