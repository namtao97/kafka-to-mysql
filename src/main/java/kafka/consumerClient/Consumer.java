package kafka.consumerClient;

import java.util.concurrent.CountDownLatch;


public interface Consumer extends Runnable {
    void shutdown();
    String topic();
    String groupID();
    String bootstrapServer();
    void setLatch(CountDownLatch latch);
}
