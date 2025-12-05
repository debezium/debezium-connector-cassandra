/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.debezium.config.Configuration;

class CassandraConnectorConfigTest {

    @Test
    void testConfigs() {
        String kafkaTopicPrefix = "test_prefix";
        CassandraConnectorConfig config = buildTaskConfig(CassandraConnectorConfig.TOPIC_PREFIX.name(), kafkaTopicPrefix);
        assertEquals(kafkaTopicPrefix, config.getLogicalName());

        String snapshotConsistency = "ALL";
        config = buildTaskConfig(CassandraConnectorConfig.SNAPSHOT_CONSISTENCY.name(), snapshotConsistency);
        assertEquals(snapshotConsistency, config.snapshotConsistencyLevel().name().toUpperCase());

        int port = 1234;
        config = buildTaskConfig(CassandraConnectorConfig.HTTP_PORT.name(), String.valueOf(port));
        assertEquals(port, config.httpPort());

        String cassandraConfig = "cassandra-unit.yaml";
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_CONFIG.name(), cassandraConfig);
        assertEquals(cassandraConfig, config.cassandraConfig());

        String kafkaServers = "host1,host2,host3";
        config = buildTaskConfig("kafka.producer.bootstrap.servers", kafkaServers);
        assertEquals(kafkaServers, config.getKafkaConfigs().getProperty("bootstrap.servers"));

        String schemaRegistry = "schema-registry-host";
        config = buildTaskConfig("kafka.producer.schema.registry", schemaRegistry);
        assertEquals(schemaRegistry, config.getKafkaConfigs().getProperty("schema.registry"));

        String offsetBackingStore = "/some/offset/backing/store/";
        config = buildTaskConfig(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR.name(), offsetBackingStore);
        assertEquals(offsetBackingStore, config.offsetBackingStoreDir());

        int offsetFlushIntervalMs = 1234;
        config = buildTaskConfig(CassandraConnectorConfig.OFFSET_FLUSH_INTERVAL_MS.name(), String.valueOf(offsetFlushIntervalMs));
        assertEquals(offsetFlushIntervalMs, config.offsetFlushIntervalMs().toMillis());

        int offsetMaxFlushSize = 200;
        config = buildTaskConfig(CassandraConnectorConfig.MAX_OFFSET_FLUSH_SIZE.name(), String.valueOf(offsetMaxFlushSize));
        assertEquals(offsetMaxFlushSize, config.maxOffsetFlushSize());

        int maxQueueSize = 500;
        config = buildTaskConfig(CassandraConnectorConfig.MAX_QUEUE_SIZE.name(), String.valueOf(maxQueueSize));
        assertEquals(maxQueueSize, config.maxQueueSize());

        int maxBatchSize = 500;
        config = buildTaskConfig(CassandraConnectorConfig.MAX_BATCH_SIZE.name(), String.valueOf(maxBatchSize));
        assertEquals(maxBatchSize, config.maxBatchSize());

        int pollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.POLL_INTERVAL_MS.name(), String.valueOf(pollIntervalMs));
        assertEquals(pollIntervalMs, config.pollInterval().toMillis());

        int schemaPollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.SCHEMA_POLL_INTERVAL_MS.name(), String.valueOf(schemaPollIntervalMs));
        assertEquals(schemaPollIntervalMs, config.schemaPollInterval().toMillis());

        int cdcDirPollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.CDC_DIR_POLL_INTERVAL_MS.name(), String.valueOf(cdcDirPollIntervalMs));
        assertEquals(cdcDirPollIntervalMs, config.cdcDirPollInterval().toMillis());

        int snapshotPollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.SNAPSHOT_POLL_INTERVAL_MS.name(), String.valueOf(snapshotPollIntervalMs));
        assertEquals(snapshotPollIntervalMs, config.snapshotPollInterval().toMillis());

        String fieldExcludeList = "keyspace1.table1.column1,keyspace1.table1.column2";
        List<String> fieldExcludeListExpected = Arrays.asList(fieldExcludeList.split(","));
        config = buildTaskConfig(CassandraConnectorConfig.FIELD_EXCLUDE_LIST.name(), fieldExcludeList);
        assertEquals(fieldExcludeListExpected, config.fieldExcludeList());

        config = buildTaskConfig(CassandraConnectorConfig.TOMBSTONES_ON_DELETE.name(), "true");
        assertTrue(config.tombstonesOnDelete());

        String snapshotMode = "always";
        config = buildTaskConfig(CassandraConnectorConfig.SNAPSHOT_MODE.name(), snapshotMode);
        assertEquals(CassandraConnectorConfig.SnapshotMode.ALWAYS, config.snapshotMode());

        String commitLogDir = "/foo/bar";
        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR.name(), commitLogDir);
        assertEquals(commitLogDir, config.commitLogRelocationDir());

        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_POST_PROCESSING_ENABLED.name(), "false");
        assertEquals(false, config.postProcessEnabled());

        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_ERROR_REPROCESSING_ENABLED.name(), "true");
        assertTrue(config.errorCommitLogReprocessEnabled());

        String transferClazz = "io.debezium.connector.cassandra.BlackHoleCommitLogTransfer";
        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_TRANSFER_CLASS.name(), transferClazz);
        assertEquals(transferClazz, config.getCommitLogTransfer().getClass().getName());

        String keyConverterClass = "io.confluent.connect.avro.AvroConverter";
        HashMap<String, Object> keyConverterConfigs = new HashMap<>();
        keyConverterConfigs.put(CassandraConnectorConfig.KEY_CONVERTER_CLASS_CONFIG.name(), keyConverterClass);
        keyConverterConfigs.put(CassandraConnectorConfig.KEY_CONVERTER_PREFIX + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config = buildTaskConfigs(keyConverterConfigs);
        assertEquals(keyConverterClass, config.getKeyConverter().getClass().getName());

        String valueConverterClass = "org.apache.kafka.connect.json.JsonConverter";
        config = buildTaskConfig(CassandraConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG.name(), valueConverterClass);
        assertEquals(valueConverterClass, config.getValueConverter().getClass().getName());

        int shutdownTimeoutSeconds = 10;
        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS.name(), String.valueOf(shutdownTimeoutSeconds));
        assertEquals(shutdownTimeoutSeconds, config.getCommitLogProcessorShutdownTimeoutSeconds());
    }

    private CassandraConnectorConfig buildTaskConfigs(HashMap<String, Object> map) {
        Configuration config = Configuration.from(map)
                .edit()
                .with(CassandraConnectorConfig.TOPIC_PREFIX, "someconnector")
                .build();

        return new CassandraConnectorConfig(config);
    }

    private CassandraConnectorConfig buildTaskConfig(String key, Object value) {
        Configuration config = Configuration.empty()
                .edit()
                .with(CassandraConnectorConfig.TOPIC_PREFIX, "someconnector")
                .with(key, value)
                .build();

        return new CassandraConnectorConfig(config);
    }

    @Test
    void testDefaultConfigs() {
        Configuration configuration = Configuration.empty()
                .edit()
                .with(CassandraConnectorConfig.TOPIC_PREFIX, "someconnector")
                .build();

        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        assertEquals(CassandraConnectorConfig.DEFAULT_SNAPSHOT_CONSISTENCY, config.snapshotConsistencyLevel().name().toUpperCase());
        assertEquals(CassandraConnectorConfig.DEFAULT_HTTP_PORT, config.httpPort());
        assertEquals(CassandraConnectorConfig.DEFAULT_MAX_QUEUE_SIZE, config.maxQueueSize());
        assertEquals(CassandraConnectorConfig.DEFAULT_MAX_BATCH_SIZE, config.maxBatchSize());
        assertEquals(CassandraConnectorConfig.DEFAULT_POLL_INTERVAL_MS, config.pollInterval().toMillis());
        assertEquals(CassandraConnectorConfig.DEFAULT_MAX_OFFSET_FLUSH_SIZE, config.maxOffsetFlushSize());
        assertEquals(CassandraConnectorConfig.DEFAULT_OFFSET_FLUSH_INTERVAL_MS, config.offsetFlushIntervalMs().toMillis());
        assertEquals(CassandraConnectorConfig.DEFAULT_SCHEMA_POLL_INTERVAL_MS, config.schemaPollInterval().toMillis());
        assertEquals(CassandraConnectorConfig.DEFAULT_CDC_DIR_POLL_INTERVAL_MS, config.cdcDirPollInterval().toMillis());
        assertEquals(CassandraConnectorConfig.DEFAULT_SNAPSHOT_POLL_INTERVAL_MS, config.snapshotPollInterval().toMillis());
        assertEquals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED, config.postProcessEnabled());
        assertEquals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_ERROR_REPROCESSING_ENABLED, config.errorCommitLogReprocessEnabled());
        assertEquals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS, config.getCommitLogTransfer().getClass().getName());
        assertEquals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS, config.getCommitLogProcessorShutdownTimeoutSeconds());
        assertFalse(config.tombstonesOnDelete());
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, config.snapshotMode());
        assertEquals(CassandraConnectorConfig.DEFAULT_LATEST_COMMIT_LOG_ONLY, config.latestCommitLogOnly());
    }

    @Test
    void testSnapshotMode() {
        String mode = "initial";
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "INITIAL";
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "Initial";
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "always";
        assertEquals(CassandraConnectorConfig.SnapshotMode.ALWAYS, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "never";
        assertEquals(CassandraConnectorConfig.SnapshotMode.NEVER, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = null;
        assertFalse(CassandraConnectorConfig.SnapshotMode.fromText(mode).isPresent());
        mode = "";
        assertFalse(CassandraConnectorConfig.SnapshotMode.fromText(mode).isPresent());
        mode = "invalid";
        assertFalse(CassandraConnectorConfig.SnapshotMode.fromText(mode).isPresent());
    }
}
