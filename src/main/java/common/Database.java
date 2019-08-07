package common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;


public class Database {

    private static final String configFile = "src\\main\\resources\\DB.properties";
    private static HikariConfig config = new HikariConfig(configFile);
    private static HikariDataSource db = new HikariDataSource(config);
    private static Logger logger = LoggerFactory.getLogger(Database.class.getName());


    private Database() {}


    public static void shutdown() {
        db.close();
    }


    public static void insertMessageToDB(List<ConsumerRecord<String, MessageObject>> records) throws SQLException {
        Connection connection = db.getConnection();

        String query = "INSERT INTO `message`(`msg`, `time`) value (?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(query);

        for (ConsumerRecord<String, MessageObject> record : records) {
            logger.info("Key: " + record.key() + ", Value: " + record.value());
            logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

            preparedStatement.setString(1, record.value().getMessage());
            preparedStatement.setLong(2, record.value().getTime());
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
        connection.commit();
    }
}
