public class ReceiveMessageFromKafkaTopic {
    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        String groupID = "group1";
        String topic = "test";

        ConsumerGroup consumerGroup = new ConsumerGroup(bootstrapServer, groupID, topic, 3, 1);
        consumerGroup.run();
    }
}
