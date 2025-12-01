/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.storage.Converter;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer.DecimalMode;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer.VarIntMode;

/**
 * All configs used by a Cassandra connector agent.
 */
public class CassandraConnectorConfig extends CommonConnectorConfig {

    /**
     * The set of predefined SnapshotMode options.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot whenever a new table with cdc enabled is detected. This is detected by periodically
         * scanning tables in Cassandra.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot for unsnapshotted tables upon initial startup of the cdc agent.
         */
        INITIAL("initial"),

        /**
         * Never perform a snapshot, instead change events are only read from commit logs.
         */
        NEVER("never");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static Optional<SnapshotMode> fromText(String text) {
            return Arrays.stream(values())
                    .filter(v -> text != null && v.name().toLowerCase().equals(text.toLowerCase()))
                    .findFirst();
        }
    }

    /**
     * The set of predefined DecimalHandlingMode options.
     */
    public enum DecimalHandlingMode implements EnumeratedValue {

        /**
         * Represent decimal values by using Java's double, which might not offer the precision but which is easy to use in consumers.
         */
        DOUBLE("double"),

        /**
         * Use java.math.BigDecimal to represent decimal values, which are encoded in the change events by using a binary
         * representation and Kafka Connect’s org.apache.kafka.connect.data.Decimal type.
         */
        PRECISE("precise"),

        /**
         * Encodes decimal values as formatted strings.
         */
        STRING("string");

        private final String value;

        DecimalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public DecimalMode asDecimalMode() {
            switch (this) {
                case PRECISE:
                    return DecimalMode.PRECISE;
                case STRING:
                    return DecimalMode.STRING;
                case DOUBLE:
                default:
                    return DecimalMode.DOUBLE;
            }
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static DecimalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (DecimalHandlingMode option : DecimalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static DecimalHandlingMode parse(String value, String defaultValue) {
            DecimalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined VarIntHandlingMode options.
     */
    public enum VarIntHandlingMode implements EnumeratedValue {

        /**
         * Represent varint values by using Java's long, which might not offer the precision but which is easy to use in consumers.
         */
        LONG("long"),

        /**
         * Use java.math.BigDecimal to represent varint values, which are encoded in the change events by using a binary
         * representation and Kafka Connect’s org.apache.kafka.connect.data.Decimal type.
         */
        PRECISE("precise"),

        /**
         * Encodes varint values as formatted strings.
         */
        STRING("string");

        private final String value;

        VarIntHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public VarIntMode asVarIntMode() {
            switch (this) {
                case PRECISE:
                    return VarIntMode.PRECISE;
                case STRING:
                    return VarIntMode.STRING;
                case LONG:
                default:
                    return VarIntMode.LONG;
            }
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static VarIntHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (VarIntHandlingMode option : VarIntHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static VarIntHandlingMode parse(String value, String defaultValue) {
            VarIntHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined EventOrderGuaranteeMode options.
     * Each option determines to which property used for hashing.
     * Events with the same hash value maintain the same order.
     * Preferred to use PARTITION_VALUES to have same hashing strategy with messages in kafka.
     */
    public enum EventOrderGuaranteeMode implements EnumeratedValue {

        /**
         * Use commit log file name to calculate the hash of the event to determine the queue index.
         */
        COMMITLOG_FILE("commitlog_file"),

        /**
         * Use partition column values to calculate the hash of event to determine queue index.
         */
        PARTITION_VALUES("partition_values");

        private final String value;

        EventOrderGuaranteeMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static EventOrderGuaranteeMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (EventOrderGuaranteeMode option : values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

    }

    /**
     * The prefix prepended to all Kafka producer configurations, including schema registry
     */
    public static final String KAFKA_PRODUCER_CONFIG_PREFIX = "kafka.producer.";

    /**
     * The prefix prepended to all Kafka key converter configurations, including schema registry.
     */
    public static final String KEY_CONVERTER_PREFIX = "key.converter.";

    /**
     * The prefix prepended to all Kafka value converter configurations, including schema registry.
     */
    public static final String VALUE_CONVERTER_PREFIX = "value.converter.";

    /**
     * The prefix for all {@link io.debezium.connector.cassandra.CommitLogTransfer} configurations.
     */
    public static final String COMMIT_LOG_TRANSFER_CONFIG_PREFIX = "commit.log.transfer.";

    public static final Field TOPIC_PREFIX = Field.create("topic.prefix")
            .withType(Type.STRING)
            .withDescription("Topic prefix for the Cassandra cluster. This name should be identical across all Cassandra connectors in a Cassandra cluster");

    public static final Field KEY_CONVERTER_CLASS_CONFIG = Field.create("key.converter")
            .withType(Type.STRING)
            .withDescription("Required config for Kafka key converter.");

    public static final Field VALUE_CONVERTER_CLASS_CONFIG = Field.create("value.converter")
            .withType(Type.STRING)
            .withDescription("Required config for Kafka value converter.");

    /**
     * Must be one of 'INITIAL', 'ALWAYS', or 'NEVER'. The default snapshot mode is 'INITIAL'.
     * See {@link SnapshotMode for details}.
     */
    public static final String DEFAULT_SNAPSHOT_MODE = "INITIAL";
    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withType(Type.STRING)
            .withDefault(DEFAULT_SNAPSHOT_MODE)
            .withDescription("Specifies the criteria for running a snapshot (eg. initial sync) upon startup of the cassandra connector agent.");

    /**
     * Specify the {@link ConsistencyLevel} used for the snapshot query.
     */
    public static final String DEFAULT_SNAPSHOT_CONSISTENCY = "QUORUM";
    public static final Field SNAPSHOT_CONSISTENCY = Field.create("snapshot.consistency")
            .withType(Type.STRING)
            .withDefault(DEFAULT_SNAPSHOT_CONSISTENCY)
            .withDescription("Specifies the ConsistencyLevel used for the snapshot query.");

    public static final int DEFAULT_HTTP_PORT = 8000;
    public static final Field HTTP_PORT = Field.create("http.port")
            .withType(Type.INT).withDefault(DEFAULT_HTTP_PORT)
            .withDescription("The port used by the HTTP server for ping, health check, and build info. Defaults to 8000.");

    public static final Field CASSANDRA_CONFIG = Field.create("cassandra.config")
            .withType(Type.STRING)
            .withDescription("The absolute path of the YAML config file used by a Cassandra node.");

    public static final Field CASSANDRA_CLUSTER_NAME = Field.create("cassandra.cluster.name")
            .withType(Type.STRING)
            .withDescription("Name of Cassandra cluster.")
            .withDefault("Test Cluster");

    public static final Field COMMIT_LOG_RELOCATION_DIR = Field.create("commit.log.relocation.dir")
            .withType(Type.STRING)
            .withValidation(Field::isRequired)
            .withDescription("The local directory which commit logs get relocated to once processed.");

    /**
     * If disabled, commit logs would not be deleted post-process, and this could lead to disk storage
     */
    public static final boolean DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED = true;
    public static final Field COMMIT_LOG_POST_PROCESSING_ENABLED = Field.create("commit.log.post.processing.enabled")
            .withType(Type.BOOLEAN)
            .withDefault(DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED)
            .withDescription("Determines whether or not the CommitLogPostProcessor should run.");

    public static final boolean DEFAULT_COMMIT_LOG_ERROR_REPROCESSING_ENABLED = false;
    public static final Field COMMIT_LOG_ERROR_REPROCESSING_ENABLED = Field.create("commit.log.error.reprocessing.enabled")
            .withType(Type.BOOLEAN)
            .withDefault(DEFAULT_COMMIT_LOG_ERROR_REPROCESSING_ENABLED)
            .withDescription("Determines whether or not the CommitLogProcessor should re-process error commitLogFiles.");

    /**
     * Only valid for Cassandra 4 and if enabled, commit logs would be read incrementally instead of reading complete log file
     */
    public static final boolean DEFAULT_COMMIT_LOG_REAL_TIME_PROCESSING_ENABLED = false;
    public static final Field COMMIT_LOG_REAL_TIME_PROCESSING_ENABLED = Field.create("commit.log.real.time.processing.enabled")
            .withType(Type.BOOLEAN)
            .withDefault(DEFAULT_COMMIT_LOG_REAL_TIME_PROCESSING_ENABLED)
            .withDescription("Enables the near real-time processing of commit logs for Cassandra 4 by reading commit log files incrementally");

    public boolean isCommitLogRealTimeProcessingEnabled() {
        return this.getConfig().getBoolean(COMMIT_LOG_REAL_TIME_PROCESSING_ENABLED);
    }

    /**
     * Only valid for Cassandra 4 and defines the polling interval to check for completeness of commit log file
     */
    public static final int DEFAULT_COMMIT_LOG_MARKED_COMPLETE_POLL_INTERVAL_IN_MS = 10_000;
    public static final Field COMMIT_LOG_MARKED_COMPLETE_POLL_INTERVAL_IN_MS = Field.create("commit.log.marked.complete.poll.interval.ms")
            .withType(Type.INT)
            .withDefault(DEFAULT_COMMIT_LOG_MARKED_COMPLETE_POLL_INTERVAL_IN_MS)
            .withDescription("Defines the polling interval to check for CommitLog file marked complete in Cassandra 4");

    public int getCommitLogMarkedCompletePollInterval() {
        return this.getConfig().getInteger(COMMIT_LOG_MARKED_COMPLETE_POLL_INTERVAL_IN_MS);
    }

    /**
     * The fully qualified {@link CommitLogTransfer} class used to transfer commit logs.
     * The default option will delete all commit log files after processing (successful or otherwise).
     * You can extend a custom implementation.
     */
    public static final String DEFAULT_COMMIT_LOG_TRANSFER_CLASS = "io.debezium.connector.cassandra.BlackHoleCommitLogTransfer";
    public static final Field COMMIT_LOG_TRANSFER_CLASS = Field.create("commit.log.transfer.class")
            .withType(Type.STRING)
            .withDefault(DEFAULT_COMMIT_LOG_TRANSFER_CLASS)
            .withDescription(
                    "A custom option used to transfer commit logs. The default option will delete all commit log files after processing (successful or otherwise).");

    public static final Field OFFSET_BACKING_STORE_DIR = Field.create("offset.backing.store.dir")
            .withType(Type.STRING)
            .withValidation(Field::isRequired)
            .withDescription("The directory which is used to store offset tracking files.");

    /**
     * The default value of 0 implies the offset will be flushed every time.
     */
    public static final int DEFAULT_OFFSET_FLUSH_INTERVAL_MS = 0;
    public static final Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
            .withType(Type.INT)
            .withDefault(DEFAULT_OFFSET_FLUSH_INTERVAL_MS)
            .withDescription("The minimum amount of time to wait before committing the offset, given in milliseconds. Defaults 0 ms.");

    /**
     * This config is effective only if offset_flush_interval_ms != 0
     */
    public static final int DEFAULT_MAX_OFFSET_FLUSH_SIZE = 100;
    public static final Field MAX_OFFSET_FLUSH_SIZE = Field.create("max.offset.flush.size")
            .withType(Type.INT)
            .withDefault(DEFAULT_MAX_OFFSET_FLUSH_SIZE)
            .withDescription("The maximum records that are allowed to be processed until it is required to flush offset to disk.");

    public static final int DEFAULT_SCHEMA_POLL_INTERVAL_MS = 10_000;
    public static final Field SCHEMA_POLL_INTERVAL_MS = Field.create("schema.refresh.interval.ms")
            .withType(Type.INT)
            .withDefault(DEFAULT_SCHEMA_POLL_INTERVAL_MS)
            .withValidation(Field::isPositiveInteger)
            .withDescription(
                    "Interval for the schema processor to wait before refreshing the cached Cassandra table schemas, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final int DEFAULT_CDC_DIR_POLL_INTERVAL_MS = 10_000;
    public static final Field CDC_DIR_POLL_INTERVAL_MS = Field.create("cdc.dir.poll.interval.ms")
            .withType(Type.INT)
            .withDefault(DEFAULT_CDC_DIR_POLL_INTERVAL_MS)
            .withDescription("The maximum amount of time to wait on each poll before re-attempt, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final int DEFAULT_SNAPSHOT_POLL_INTERVAL_MS = 10_000;
    public static final Field SNAPSHOT_POLL_INTERVAL_MS = Field.create("snapshot.scan.interval.ms")
            .withType(Type.INT)
            .withDefault(DEFAULT_SNAPSHOT_POLL_INTERVAL_MS)
            .withValidation(Field::isPositiveInteger)
            .withDescription(
                    "Interval for the snapshot processor to wait before re-scanning tables to look for new cdc-enabled tables. Defaults to 10 seconds (10,000 ms).");

    public static final int DEFAULT_COMMIT_LOG_RELOCATION_DIR_POLL_INTERVAL_MS = 10_000;
    public static final Field COMMIT_LOG_RELOCATION_DIR_POLL_INTERVAL_MS = Field.create("commit.log.relocation.dir.poll.interval.ms")
            .withType(Type.INT)
            .withDefault(DEFAULT_COMMIT_LOG_RELOCATION_DIR_POLL_INTERVAL_MS)
            .withDescription(
                    "The amount of time the CommitLogPostProcessor should wait to re-fetch all commitLog files in relocation dir, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final int DEFAULT_NUM_OF_CHANGE_EVENT_QUEUES = 1;
    public static final Field NUM_OF_CHANGE_EVENT_QUEUES = Field.create("num.of.change.event.queues")
            .withType(Type.INT)
            .withDefault(DEFAULT_NUM_OF_CHANGE_EVENT_QUEUES)
            .withDescription(
                    "The number of change event queues and queue processors.");

    /**
     * A comma-separated list of fully-qualified names of fields that should be excluded from change event message values.
     * Fully-qualified names for fields are in the form {@code <keyspace_name>.<field_name>.<nested_field_name>}.
     */
    public static final Field FIELD_EXCLUDE_LIST = Field.create("field.exclude.list")
            .withDisplayName("Exclude Fields")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withInvisibleRecommender()
            .withDescription("Regular expressions matching fields to include in change events");

    public static final Field CASSANDRA_DRIVER_CONFIG_FILE = Field.create("cassandra.driver.config.file")
            .withDisplayName("Cassandra Driver Configuration File")
            .withType(Type.STRING)
            .withDefault("application.conf")
            .withDescription("Path to Cassandra driver configuration file");

    /**
     * Instead of parsing commit logs from CDC directory, this will look for the commit log with the
     * latest modified timestamp in the commit log directory and attempt to process this file only.
     * Only used for Testing!
     */
    public static final boolean DEFAULT_LATEST_COMMIT_LOG_ONLY = false;
    public static final Field LATEST_COMMIT_LOG_ONLY = Field.create("latest.commit.log.only")
            .withType(Type.BOOLEAN)
            .withDefault(DEFAULT_LATEST_COMMIT_LOG_ONLY)
            .withDescription("Fetch the commit log with the latest modified timestamp in the commit log directory.");

    public static final int DEFAULT_POLL_INTERVAL_MS = 1000;

    public static final boolean DEFAULT_TOMBSTONES_ON_DELETE = false;

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 0;

    /**
     * Must be one of 'DOUBLE', 'PRECISE', or 'STRING'. The default decimal handling mode is 'DOUBLE'.
     * See {@link DecimalHandlingMode for details}.
     */
    public static final Field DECIMAL_HANDLING_MODE = Field.create("decimal.handling.mode")
            .withDisplayName("Decimal Handling")
            .withEnum(DecimalHandlingMode.class, DecimalHandlingMode.DOUBLE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies how Cassandra decimal columns should be represented in change events.");

    /**
     * Must be one of 'LONG', 'PRECISE', or 'STRING'. The default varint handling mode is 'LONG'.
     * See {@link VarIntHandlingMode for details}.
     */
    public static final Field VARINT_HANDLING_MODE = Field.create("varint.handling.mode")
            .withDisplayName("VarInt Handling")
            .withEnum(VarIntHandlingMode.class, VarIntHandlingMode.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies how Cassandra varint columns should be represented in change events.");

    static final Field CASSANDRA_NODE_ID = Field.create("cassandra.node.id")
            .withDisplayName("Cassandra node Id")
            .withType(Type.STRING)
            .withDescription("Id of the Cassandra node - must be unique among all Cassandra connectors running on all nodes");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(CassandraSourceInfoStructMaker.class.getName());

    /**
     * Must be one of 'COMMITLOG_FILE', or 'PARTITION_VALUES'. The default order guarantee mode is 'COMMITLOG_FILE'.
     * See {@link EventOrderGuaranteeMode for details}.
     */
    public static final Field EVENT_ORDER_GUARANTEE_MODE = Field.create("event.order.guarantee.mode")
            .withDisplayName("Event order guarantee")
            .withEnum(EventOrderGuaranteeMode.class, EventOrderGuaranteeMode.COMMITLOG_FILE)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies how grantee order of change events.");

    /**
     * The maximum time to wait for commit log processors to complete their tasks during shutdown.
     * If processors don't finish within this timeout, they will be forcefully cancelled.
     * Defaults to 5 seconds.
     */
    public static final int DEFAULT_COMMIT_LOG_PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS = 5;
    public static final Field COMMIT_LOG_PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS = Field.createInternal("commit.log.processor.shutdown.timeout.seconds")
            .withDisplayName("Commit Log Processor Shutdown Timeout")
            .withType(Type.INT)
            .withDefault(DEFAULT_COMMIT_LOG_PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription(
                    "The maximum time to wait for commit log processors to complete their tasks during shutdown. If processors don't finish within this timeout, they will be forcefully cancelled.");

    private static List<Field> validationFieldList = new ArrayList<>(
            Arrays.asList(OFFSET_BACKING_STORE_DIR, COMMIT_LOG_RELOCATION_DIR, SCHEMA_POLL_INTERVAL_MS, SNAPSHOT_POLL_INTERVAL_MS));

    public CassandraConnectorConfig(Configuration config) {
        super(config, DEFAULT_SNAPSHOT_FETCH_SIZE);
    }

    public Properties getKafkaConfigs() {
        Properties props = new Properties();

        // default configs
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        this.getConfig().asMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(KAFKA_PRODUCER_CONFIG_PREFIX))
                .forEach(entry -> {
                    String k = entry.getKey().replace(KAFKA_PRODUCER_CONFIG_PREFIX, "");
                    Object v = entry.getValue();
                    props.put(k, v);
                });

        return props;
    }

    public Properties commitLogTransferConfigs() {
        Properties props = new Properties();
        this.getConfig().asMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(COMMIT_LOG_TRANSFER_CONFIG_PREFIX))
                .forEach(entry -> {
                    String k = entry.getKey().replace(COMMIT_LOG_TRANSFER_CONFIG_PREFIX, "");
                    Object v = entry.getValue();
                    props.put(k, v);
                });
        return props;
    }

    public boolean latestCommitLogOnly() {
        return this.getConfig().getBoolean(LATEST_COMMIT_LOG_ONLY);
    }

    public SnapshotMode snapshotMode() {
        String mode = this.getConfig().getString(SNAPSHOT_MODE);
        Optional<SnapshotMode> snapshotModeOpt = SnapshotMode.fromText(mode);
        return snapshotModeOpt.orElseThrow(() -> new CassandraConnectorConfigException(mode + " is not a valid SnapshotMode"));
    }

    public ConsistencyLevel snapshotConsistencyLevel() {
        String cl = this.getConfig().getString(SNAPSHOT_CONSISTENCY);
        return DefaultConsistencyLevel.valueOf(cl);
    }

    public int httpPort() {
        return this.getConfig().getInteger(HTTP_PORT);
    }

    public String cassandraConfig() {
        return this.getConfig().getString(CASSANDRA_CONFIG);
    }

    public String clusterName() {
        return this.getConfig().getString(CASSANDRA_CLUSTER_NAME);
    }

    public String commitLogRelocationDir() {
        return this.getConfig().getString(COMMIT_LOG_RELOCATION_DIR);
    }

    public boolean postProcessEnabled() {
        return this.getConfig().getBoolean(COMMIT_LOG_POST_PROCESSING_ENABLED);
    }

    public boolean errorCommitLogReprocessEnabled() {
        return this.getConfig().getBoolean(COMMIT_LOG_ERROR_REPROCESSING_ENABLED);
    }

    public CommitLogTransfer getCommitLogTransfer() {
        try {
            String clazz = this.getConfig().getString(COMMIT_LOG_TRANSFER_CLASS);
            CommitLogTransfer transfer = (CommitLogTransfer) Class.forName(clazz).newInstance();
            transfer.init(commitLogTransferConfigs());
            return transfer;
        }
        catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    public String offsetBackingStoreDir() {
        return this.getConfig().getString(OFFSET_BACKING_STORE_DIR);
    }

    public Duration offsetFlushIntervalMs() {
        int ms = this.getConfig().getInteger(OFFSET_FLUSH_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public long maxOffsetFlushSize() {
        return this.getConfig().getLong(MAX_OFFSET_FLUSH_SIZE);
    }

    public int maxQueueSize() {
        return this.getConfig().getInteger(MAX_QUEUE_SIZE);
    }

    public int maxBatchSize() {
        return this.getConfig().getInteger(MAX_BATCH_SIZE);
    }

    public String cassandraDriverConfig() {
        return this.getConfig().getString(CASSANDRA_DRIVER_CONFIG_FILE);
    }

    /**
     * Positive integer value that specifies the number of milliseconds the commit log processor should wait during
     * each iteration for new change events to appear in the queue. Defaults to 1000 milliseconds, or 1 second.
     */
    public Duration pollInterval() {
        int ms = this.getConfig().getInteger(POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration schemaPollInterval() {
        int ms = this.getConfig().getInteger(SCHEMA_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration cdcDirPollInterval() {
        int ms = this.getConfig().getInteger(CDC_DIR_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration snapshotPollInterval() {
        int ms = this.getConfig().getInteger(SNAPSHOT_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration commitLogRelocationDirPollInterval() {
        int ms = this.getConfig().getInteger(COMMIT_LOG_RELOCATION_DIR_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public int numOfChangeEventQueues() {
        return this.getConfig().getInteger(NUM_OF_CHANGE_EVENT_QUEUES);
    }

    public List<String> fieldExcludeList() {
        String fieldExcludeList = this.getConfig().getString(FIELD_EXCLUDE_LIST);
        if (fieldExcludeList == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(fieldExcludeList.split(","));
    }

    /**
     * Whether deletion events should have a subsequent tombstone event (true) or not (false).
     * It's important to note that in Cassandra, two events with the same key may be updating
     * different columns of a given table. So this could potentially result in records being lost
     * during compaction if they haven't been consumed by the consumer yet. In other words, do NOT
     * set this to true if you have kafka compaction turned on.
     */
    public boolean tombstonesOnDelete() {
        return this.getConfig().getBoolean(TOMBSTONES_ON_DELETE, DEFAULT_TOMBSTONES_ON_DELETE);
    }

    public DecimalMode getDecimalMode() {
        return DecimalHandlingMode
                .parse(this.getConfig().getString(DECIMAL_HANDLING_MODE))
                .asDecimalMode();
    }

    public VarIntMode getVarIntMode() {
        return VarIntHandlingMode
                .parse(this.getConfig().getString(VARINT_HANDLING_MODE))
                .asVarIntMode();
    }

    public Converter getKeyConverter() throws CassandraConnectorConfigException {
        try {
            Class keyConverterClass = Class.forName(this.getConfig().getString(KEY_CONVERTER_CLASS_CONFIG));
            Converter keyConverter = (Converter) keyConverterClass.newInstance();
            Map<String, Object> keyConverterConfigs = keyValueConverterConfigs(KEY_CONVERTER_PREFIX);
            keyConverter.configure(keyConverterConfigs, true);
            return keyConverter;
        }
        catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    public Converter getValueConverter() throws CassandraConnectorConfigException {
        try {
            Class valueConverterClass = Class.forName(this.getConfig().getString(VALUE_CONVERTER_CLASS_CONFIG));
            Converter valueConverter = (Converter) valueConverterClass.newInstance();
            Map<String, Object> valueConverterConfigs = keyValueConverterConfigs(VALUE_CONVERTER_PREFIX);
            valueConverter.configure(valueConverterConfigs, false);
            return valueConverter;
        }
        catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    private Map<String, Object> keyValueConverterConfigs(String converterPrefix) {
        return this.getConfig().asMap().entrySet().stream()
                .filter(entry -> entry.toString().startsWith(converterPrefix))
                .collect(Collectors.toMap(entry -> entry.getKey().replace(converterPrefix, ""), entry -> entry.getValue()));
    }

    @Override
    public String toString() {
        return this.getConfig().asMap().entrySet().stream()
                .filter(e -> !e.getKey().toLowerCase().contains("username") && !e.getKey().toLowerCase().contains("password"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .toString();
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public EnumeratedValue getSnapshotMode() {
        return snapshotMode();
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    public Field.Set getValidationFieldSet() {
        return Field.setOf(validationFieldList);
    }

    public void setValidationFieldList(List<Field> validationFieldList) {
        this.validationFieldList = validationFieldList;
    }

    public String getNodeId() {
        return this.getConfig().getString(CASSANDRA_NODE_ID);
    }

    public EventOrderGuaranteeMode getEventOrderGuaranteeMode() {
        return EventOrderGuaranteeMode.parse(this.getConfig().getString(EVENT_ORDER_GUARANTEE_MODE));
    }

    public int getCommitLogProcessorShutdownTimeoutSeconds() {
        return this.getConfig().getInteger(COMMIT_LOG_PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS);
    }
}
