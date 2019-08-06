public class ReceiveMessageFromKafkaTopic {

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String groupID = "group1";
        String topic = "test";
        int numberOfConsumer = 3;
        int minBatchSize = 1;

        /**
         * The number of consumer threads is same the numer of topic partitions. In this demo is 3 partitions.
         * @param numberOfConsumer - number of consumer run in a consumer group
         * @param minBatchSize - the minimum number of message to insert to database
        */
        ConsumerGroup consumerGroup = new ConsumerGroup(bootstrapServer, groupID, topic, numberOfConsumer, minBatchSize);
        consumerGroup.run();
    }
}
