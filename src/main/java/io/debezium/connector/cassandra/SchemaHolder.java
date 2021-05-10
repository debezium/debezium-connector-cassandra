/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

/**
 * Caches the key and value schema for all CDC-enabled tables. This cache gets updated
 * by {@link SchemaProcessor} periodically.
 */
public class SchemaHolder {
    private static final String NAMESPACE = "io.debezium.connector.cassandra";
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHolder.class);

    public final Map<KeyspaceTable, KeyValueSchema> tableToKVSchemaMap = new ConcurrentHashMap<>();

    public final String kafkaTopicPrefix;
    public final SourceInfoStructMaker sourceInfoStructMaker;
    private final CassandraClient cassandraClient;

    public SchemaHolder(CassandraClient cassandraClient, String kafkaTopicPrefix, SourceInfoStructMaker sourceInfoStructMaker) {
        this.cassandraClient = cassandraClient;
        this.kafkaTopicPrefix = kafkaTopicPrefix;
        this.sourceInfoStructMaker = sourceInfoStructMaker;
    }

    public synchronized KeyValueSchema getKeyValueSchema(KeyspaceTable kt) {
        return tableToKVSchemaMap.getOrDefault(kt, null);
    }

    public Set<TableMetadata> getCdcEnabledTableMetadataSet() {
        return cassandraClient
                .getCluster()
                .getMetadata()
                .getKeyspaces()
                .stream()
                .map(KeyspaceMetadata::getTables)
                .flatMap(Collection::stream)
                .filter(t -> t.getOptions().isCDC())
                .collect(Collectors.toSet());
    }

    public synchronized void removeSchemasOfAllTablesInKeyspace(String keyspace) {
        final List<KeyspaceTable> collect = tableToKVSchemaMap.keySet()
                .stream()
                .filter(keyValueSchema -> keyValueSchema.keyspace.equals(keyspace))
                .collect(toList());

        collect.forEach(tableToKVSchemaMap::remove);
    }

    public synchronized void removeTableSchema(KeyspaceTable kst) {
        tableToKVSchemaMap.remove(kst);
    }

    // there is not "addKeyspace", it is not necessary
    // as we will ever add a concrete table (with keyspace) but we will also dropping all tables when keyspace is dropped
    public synchronized void addOrUpdateTableSchema(KeyspaceTable kst, KeyValueSchema kvs) {
        tableToKVSchemaMap.put(kst, kvs);
    }

    /**
     * Get the schema of an inner field based on the field name
     * @param fieldName the name of the field in the schema
     * @param schema    the schema where the field resides in
     * @return Schema
     */
    public static Schema getFieldSchema(String fieldName, Schema schema) {
        if (schema.type().equals(Schema.Type.STRUCT)) {
            return schema.field(fieldName).schema();
        }
        throw new CassandraConnectorSchemaException("Only STRUCT type is supported for this method, but encountered " + schema.type());
    }

    public static class KeyValueSchema {
        private final TableMetadata tableMetadata;
        private final Schema keySchema;
        private final Schema valueSchema;

        KeyValueSchema(String kafkaTopicPrefix, TableMetadata tableMetadata, SourceInfoStructMaker sourceInfoStructMaker) {
            this.tableMetadata = tableMetadata;
            this.keySchema = getKeySchema(kafkaTopicPrefix, tableMetadata);
            this.valueSchema = getValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker);
        }

        public TableMetadata tableMetadata() {
            return tableMetadata;
        }

        public Schema keySchema() {
            return keySchema;
        }

        public Schema valueSchema() {
            return valueSchema;
        }

        private Schema getKeySchema(String kafkaTopicPrefix, TableMetadata tm) {
            if (tm == null) {
                return null;
            }
            SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(NAMESPACE + "." + getKeyName(kafkaTopicPrefix, tm));
            for (ColumnMetadata cm : tm.getPrimaryKey()) {
                AbstractType<?> convertedType = CassandraTypeConverter.convert(cm.getType());
                Schema colSchema = CassandraTypeDeserializer.getSchemaBuilder(convertedType).build();
                if (colSchema != null) {
                    schemaBuilder.field(cm.getName(), colSchema);
                }
            }
            return schemaBuilder.build();
        }

        private Schema getValueSchema(String kafkaTopicPrefix, TableMetadata tm, SourceInfoStructMaker sourceInfoStructMaker) {
            if (tm == null) {
                return null;
            }
            return SchemaBuilder.struct().name(NAMESPACE + "." + getValueName(kafkaTopicPrefix, tm))
                    .field(Record.TIMESTAMP, Schema.INT64_SCHEMA)
                    .field(Record.OPERATION, Schema.STRING_SCHEMA)
                    .field(Record.SOURCE, sourceInfoStructMaker.schema())
                    .field(Record.AFTER, RowData.rowSchema(tm))
                    .build();
        }

        private static String getKeyName(String kafkaTopicPrefix, TableMetadata tm) {
            return kafkaTopicPrefix + "." + tm.getKeyspace().getName() + "." + tm.getName() + ".Key";
        }

        private static String getValueName(String kafkaTopicPrefix, TableMetadata tm) {
            return kafkaTopicPrefix + "." + tm.getKeyspace().getName() + "." + tm.getName() + ".Value";
        }
    }
}
