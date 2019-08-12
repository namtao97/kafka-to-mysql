package database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import common.MessageObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;


public class MySQLDatabase implements Database {

    private HikariDataSource db;
    private final Logger logger = LoggerFactory.getLogger(Database.class.getName());


    public MySQLDatabase(String configFile) {
        HikariConfig config = new HikariConfig(configFile);
        db = new HikariDataSource(config);
    }


    @Override
    public void shutdown() {
        db.close();
    }


    @Override
    public void insertMessageToDB(List<ConsumerRecord<String, MessageObject>> records) throws SQLException {
        Connection connection = db.getConnection();

        String query = "INSERT IGNORE INTO `message`(`partition`, `offset`, `msg`, `time`) VALUE (?, ?, ?, ?)";
        PreparedStatement preparedStatementInsert = connection.prepareStatement(query);

        ConsumerRecord<String, MessageObject> lastRecord = null;
        HashMap<Integer, Long> offsetMap = new HashMap<>();

        for (ConsumerRecord<String, MessageObject> record : records) {
            logger.info("Key: " + record.key() + ", Value: " + record.value() + " - Partition: " + record.partition() + ", Offset: " + record.offset()) ;

            preparedStatementInsert.setInt(1, record.partition());
            preparedStatementInsert.setLong(2, record.offset());
            preparedStatementInsert.setString(3, record.value().getMessage());
            preparedStatementInsert.setLong(4, record.value().getTime());
            preparedStatementInsert.addBatch();

            if (offsetMap.get(record.partition()) == null || offsetMap.get(record.partition()) < record.offset()) {
                offsetMap.put(record.partition(), record.offset());
            }

            lastRecord = record;
        }

        preparedStatementInsert.executeBatch();

        if (lastRecord != null) {
            PreparedStatement preparedStatementSaveOffset = getSaveOffsetStatement(connection, lastRecord.topic(), offsetMap);
            preparedStatementSaveOffset.executeBatch();
        }

        connection.commit();
        connection.close();
    }


    @Override
    public void saveOffset(String topic, int partition, long offset) throws SQLException {
        Connection connection = db.getConnection();

        HashMap<Integer, Long> offsetMap = new HashMap<>(partition, offset);
        PreparedStatement preparedStatement = getSaveOffsetStatement(connection, topic, offsetMap);
        preparedStatement.executeBatch();

        connection.commit();
        connection.close();
    }


    @Override
    public long getOffset(String topic, int partition) throws SQLException {
        Connection connection = db.getConnection();

        String query = "SELECT `offset` FROM `offsets` WHERE `topic` = ? AND `partition` = ? LIMIT 1";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1, topic);
        preparedStatement.setInt(2, partition);
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet.next()) {
            return resultSet.getLong("offset") + 1L;
        }

        resultSet.close();
        connection.close();

        return 0;
    }


    private PreparedStatement getSaveOffsetStatement(Connection connection, String topic, HashMap<Integer, Long> offsetMap) throws SQLException {
        String query = "INSERT INTO `offsets`(`topic`, `partition`, `offset`) value (?, ?, ?) ON DUPLICATE KEY UPDATE `offset` = ?";

        PreparedStatement preparedStatement = connection.prepareStatement(query);

        Long offset;
        for (Integer partition : offsetMap.keySet()) {
            offset = offsetMap.get(partition);
            preparedStatement.setString(1, topic);
            preparedStatement.setInt(2, partition);
            preparedStatement.setLong(3, offset);
            preparedStatement.setLong(4, offset);
            preparedStatement.addBatch();
        }

        return preparedStatement;
    }
}
