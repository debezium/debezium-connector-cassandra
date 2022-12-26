/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.google.common.collect.ImmutableMap.of;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class TestUtils {

    public static final String TEST_CONNECTOR_NAME = "cassandra-01";
    public static final String TEST_CASSANDRA_YAML_CONFIG = "cassandra-unit.yaml";
    public static final String TEST_CASSANDRA_HOSTS = "127.0.0.1";
    public static final int TEST_CASSANDRA_PORT = 9042;
    public static final String TEST_KAFKA_SERVERS = "localhost:9092";
    public static final String TEST_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final String TEST_KAFKA_TOPIC_PREFIX = "test_topic";

    public static final String TEST_KEYSPACE_NAME = "test_keyspace";
    public static final String TEST_KEYSPACE_NAME_2 = "test_keyspace2";

    public static String TEST_TABLE_NAME = "table_" + UUID.randomUUID().toString().replace("-", "");
    public static String TEST_TABLE_NAME_2 = "table2_" + UUID.randomUUID().toString().replace("-", "");

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

    public static HashMap<String, Object> propertiesForContext() throws IOException {
        return new HashMap<>() {
            {
                put(CassandraConnectorConfig.TOPIC_PREFIX.name(), TEST_CONNECTOR_NAME);
                put(CassandraConnectorConfig.CASSANDRA_CONFIG.name(), Paths.get("src/test/resources/cassandra-unit-for-context.yaml").toAbsolutePath().toString());
                put(CassandraConnectorConfig.TOPIC_PREFIX.name(), TEST_KAFKA_TOPIC_PREFIX);
                put(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR.name(), Files.createTempDirectory("offset").toString());
                put(CassandraConnectorConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_KAFKA_SERVERS);
                put(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR.name(), Files.createTempDirectory("cdc_raw_relocation").toString());
                put(CassandraConnectorConfig.KEY_CONVERTER_CLASS_CONFIG.name(), "org.apache.kafka.connect.json.JsonConverter");
                put(CassandraConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG.name(), "org.apache.kafka.connect.json.JsonConverter");
                put(CassandraConnectorConfig.CASSANDRA_DRIVER_CONFIG_FILE.name(), Paths.get("src/test/resources/application.conf").toAbsolutePath().toString());
                // put(CassandraConnectorConfig.MAX_QUEUE_SIZE.name(), 1_000_000);
                // put(CassandraConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES.name(), 1_000_000_000);
            }
        };
    }

    public static void createTestKeyspace() throws Exception {
        createTestKeyspace(TEST_KEYSPACE_NAME);
    }

    public static void createTestKeyspace(String keyspace) throws Exception {
        try (CqlSession session = CqlSession.builder().build()) {
            session.execute(SchemaBuilder.createKeyspace(keyspace)
                    .ifNotExists()
                    .withNetworkTopologyStrategy(of("datacenter1", 1))
                    .build());
        }
    }

    public static List<String> getTables(String keyspace, CqlSession session) {
        return session.getMetadata()
                .getKeyspace(keyspace).get()
                .getTables()
                .values()
                .stream()
                .map(tmd -> tmd.getName().toString())
                .collect(Collectors.toList());
    }

    public static void truncateTestKeyspaceTableData() {
        truncateTestKeyspaceTableData(TEST_KEYSPACE_NAME);
    }

    public static void truncateTestKeyspaceTableData(String keyspace) {
        try (CqlSession session = CqlSession.builder().build()) {
            for (String table : getTables(keyspace, session)) {
                session.execute(SimpleStatement.newInstance(String.format("TRUNCATE %s.%s", keyspace, table)));
            }
        }
    }

    public static void deleteTestKeyspaceTables() throws Exception {
        deleteTestKeyspaceTables(TEST_KEYSPACE_NAME);
    }

    public static void deleteTestKeyspaceTables(String keyspaceName) throws Exception {
        try (CqlSession session = CqlSession.builder().build()) {
            for (String table : getTables(keyspaceName, session)) {
                session.execute(SimpleStatement.newInstance(String.format("DROP TABLE IF EXISTS %s.%s", keyspaceName, table)));
            }
        }
    }

    public static void runCql(String statement) {
        runCql(SimpleStatement.builder(statement).build());
    }

    public static void runCql(SimpleStatement statement) {
        try (CqlSession session = CqlSession.builder().build()) {
            session.execute(statement);
        }
    }

    /**
     * Delete all files in offset backing store directory
     */
    public static void deleteTestOffsets(CassandraConnectorContext context) throws IOException {
        String offsetDirPath = context.getCassandraConnectorConfig().offsetBackingStoreDir();
        File offsetDir = new File(offsetDirPath);
        if (offsetDir.isDirectory()) {
            File[] files = offsetDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    Files.delete(f.toPath());
                }
            }
        }
    }

    /**
     * Return the full name of the test table in the form of <keyspace>.<table>
     */
    public static String keyspaceTable(String tableName) {
        return TEST_KEYSPACE_NAME + "." + tableName;
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
}
