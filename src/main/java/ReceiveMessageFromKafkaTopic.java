import kafka.consumerClient.Consumer;
import kafka.consumerClient.ConsumerGroup;
import kafka.consumerClient.ConsumerThread;
import kafka.helper.OffsetManager;

import java.util.ArrayList;
import java.util.List;

public class ReceiveMessageFromKafkaTopic {

    public static void main(String[] args) {
        /**
         * The number of consumer threads is same the numer of topic partitions. In this demo is 3 partitions.
         * @param bootstrapServer - the address of the message brokers
         * @param groupID - the group name of the consumer group
         * @param topic - the topic name of the message brokers
         * @param numberOfConsumer - number of consumer run in a consumer group
         * @param minBatchSize - the minimum number of message to insert to database
        */
        String bootstrapServer = "localhost:9092";
        String groupID = "group1";
        String topic = "test";
        int numberOfConsumer = 3;
        int minBatchSize = 1;

        OffsetManager offsetManager = new OffsetManager("storage-offset");
        ConsumerGroup consumerGroup = new ConsumerGroup(bootstrapServer, groupID, topic);

        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numberOfConsumer; i++) {
            consumers.add(new ConsumerThread(bootstrapServer, groupID, topic, offsetManager, minBatchSize));
        }

        consumerGroup.assignListConsumers(consumers);
        consumerGroup.run();
    }
}

// need use template for message object