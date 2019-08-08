package kafka.consumerClient;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CountDownLatch;


public interface Consumer extends Runnable {
    void shutdown();
    String topic();
    String groupID();
    String bootstrapServer();
    void setLatch(CountDownLatch latch);
    long position(TopicPartition partition);
    void seek(TopicPartition partition, long offset);
}
