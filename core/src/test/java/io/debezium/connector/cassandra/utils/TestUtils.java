/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

import io.debezium.connector.cassandra.CassandraConnectorConfig;
import io.debezium.connector.cassandra.CommitLogUtil;

public class TestUtils {
    public static final String TEST_CONNECTOR_NAME = "cassandra-01";
    public static final String TEST_KAFKA_TOPIC_PREFIX = "test_topic";
    public static final String TEST_KAFKA_SERVERS = "localhost:9092";

    public static Properties generateDefaultConfigMap() throws IOException {
        Properties props = new Properties();
        props.put(CassandraConnectorConfig.TOPIC_PREFIX.name(), TEST_CONNECTOR_NAME);
        props.put(CassandraConnectorConfig.CASSANDRA_CONFIG.name(), Paths.get("src/test/resources/cassandra-unit-for-context.yaml").toAbsolutePath().toString());
        props.put(CassandraConnectorConfig.TOPIC_PREFIX.name(), TEST_KAFKA_TOPIC_PREFIX);
        props.put(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR.name(), Files.createTempDirectory("offset").toString());
        props.put(CassandraConnectorConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_KAFKA_SERVERS);
        props.put(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR.name(), Files.createTempDirectory("cdc_raw_relocation").toString());
        props.put(CassandraConnectorConfig.KEY_CONVERTER_CLASS_CONFIG.name(), "org.apache.kafka.connect.json.JsonConverter");
        props.put(CassandraConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG.name(), "org.apache.kafka.connect.json.JsonConverter");
        props.put(CassandraConnectorConfig.CASSANDRA_DRIVER_CONFIG_FILE.name(), Paths.get("src/test/resources/application.conf").toAbsolutePath().toString());
        // props.put(CassandraConnectorConfig.MAX_QUEUE_SIZE.name(), 1_000_000);
        // props.put(CassandraConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES.name(), 1_000_000_000);
        return props;
    }

    /**
     * Delete all commit log files in directory
     */
    public static void clearCommitLogFromDirectory(File directory, boolean recursive) throws IOException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(directory + " is not a valid directory");
        }

        File[] commitLogs = CommitLogUtil.getCommitLogs(directory);
        for (File commitLog : commitLogs) {
            CommitLogUtil.deleteCommitLog(commitLog);
        }

        if (recursive) {
            File[] directories = directory.listFiles(File::isDirectory);
            if (directories != null) {
                for (File dir : directories) {
                    clearCommitLogFromDirectory(dir, true);
                }
            }
        }
    }

    /**
     * Generate commit log files in directory
     */
    public static void populateFakeCommitLogsForDirectory(int numOfFiles, File directory) throws IOException {
        if (directory.exists() && !directory.isDirectory()) {
            throw new IOException(directory + " is not a directory");
        }
        if (!directory.exists() && !directory.mkdir()) {
            throw new IOException("Cannot create directory " + directory);
        }
        clearCommitLogFromDirectory(directory, true);
        long prefix = System.currentTimeMillis();
        for (int i = 0; i < numOfFiles; i++) {
            long ts = prefix + i;
            Path path = Paths.get(directory.getAbsolutePath(), "CommitLog-6-" + ts + ".log");
            boolean success = path.toFile().createNewFile();
            if (!success) {
                throw new IOException("Failed to create new commit log for testing");
            }
        }
    }
}
