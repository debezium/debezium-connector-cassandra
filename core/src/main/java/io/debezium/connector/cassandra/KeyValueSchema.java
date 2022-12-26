/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

/**
 * This class contains methods to build Kafka Key and Value Schemas from Cassandra table schemas.
 */
public class KeyValueSchema {

    private final TableMetadata tableMetadata;
    private final Schema keySchema;
    private final Schema valueSchema;

    KeyValueSchema(TableMetadata tableMetadata, Schema keySchema, Schema valueSchema) {
        this.tableMetadata = tableMetadata;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public static class KeyValueSchemaBuilder {
        private static final String NAMESPACE = "io.debezium.connector.cassandra";
        private String keyspace;
        private String table;
        private TableMetadata tableMetadata;
        private String kafkaTopicPrefix;
        private SourceInfoStructMaker<?> sourceInfoStructMaker;
        private List<String> primaryKeyNames;
        private List<Schema> primaryKeySchemas;
        private Schema rowSchema;

        public KeyValueSchemaBuilder withKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public KeyValueSchemaBuilder withTable(String table) {
            this.table = table;
            return this;
        }

        public KeyValueSchemaBuilder withKafkaTopicPrefix(String kafkaTopicPrefix) {
            this.kafkaTopicPrefix = kafkaTopicPrefix;
            return this;
        }

        public KeyValueSchemaBuilder withSourceInfoStructMarker(SourceInfoStructMaker<?> sourceInfoStructMarker) {
            this.sourceInfoStructMaker = sourceInfoStructMarker;
            return this;
        }

        public KeyValueSchemaBuilder withPrimaryKeyNames(List<String> primaryKeyNames) {
            this.primaryKeyNames = primaryKeyNames;
            return this;
        }

        public KeyValueSchemaBuilder withPrimaryKeySchemas(List<Schema> primaryKeySchemas) {
            this.primaryKeySchemas = primaryKeySchemas;
            return this;
        }

        public KeyValueSchemaBuilder withRowSchema(Schema rowSchema) {
            this.rowSchema = rowSchema;
            return this;
        }

        public KeyValueSchemaBuilder withTableMetadata(TableMetadata tableMetadata) {
            if (tableMetadata != null) {
                this.tableMetadata = tableMetadata;
                this.keyspace = this.tableMetadata.getKeyspace().toString();
                this.table = this.tableMetadata.getName().toString();
            }
            return this;
        }

        private String getKeyName() {
            return kafkaTopicPrefix + "." + keyspace + "." + table + ".Key";
        }

        private String getValueName() {
            return kafkaTopicPrefix + "." + keyspace + "." + table + ".Envelope";
        }

        private Schema getKeySchema() {
            SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(NAMESPACE + "." + getKeyName());

            for (int i = 0; i < primaryKeyNames.size(); i++) {
                schemaBuilder.field(primaryKeyNames.get(i), primaryKeySchemas.get(i));
            }

            return schemaBuilder.build();
        }

        private Schema getValueSchema() {
            return SchemaBuilder.struct().name(NAMESPACE + "." + getValueName())
                    .field(Record.TIMESTAMP, Schema.INT64_SCHEMA)
                    .field(Record.OPERATION, Schema.STRING_SCHEMA)
                    .field(Record.SOURCE, sourceInfoStructMaker.schema())
                    .field(Record.AFTER, rowSchema)
                    .build();
        }

        public KeyValueSchema build() {
            if (primaryKeyNames == null || primaryKeySchemas == null) {
                if (tableMetadata == null) {
                    throw new IllegalStateException("Unable to build, tableMetadata are null and either " +
                            "primaryKeyNames and/or primaryKeyValues are not set.");
                }
            }
            if (primaryKeyNames == null) {
                primaryKeyNames = KeyValueSchema.getPrimaryKeyNames(tableMetadata);
            }

            if (primaryKeySchemas == null) {
                primaryKeySchemas = KeyValueSchema.getPrimaryKeySchemas(tableMetadata);
            }

            if (rowSchema == null) {
                if (tableMetadata != null) {
                    rowSchema = RowData.rowSchema(tableMetadata);
                }
                else {
                    throw new IllegalStateException("Unable to build, rowSchema is not set and table metadata are null");
                }
            }

            if (keyspace == null) {
                if (tableMetadata == null) {
                    throw new IllegalStateException("Keyspace is not set and TableMetadata is not set either");
                }
                else {
                    keyspace = tableMetadata.getKeyspace().toString();
                }
            }

            if (table == null) {
                if (tableMetadata == null) {
                    throw new IllegalStateException("Table is not set and TableMetadata is not either");
                }
                else {
                    table = tableMetadata.getName().toString();
                }
            }

            return new KeyValueSchema(tableMetadata, getKeySchema(), getValueSchema());
        }
    }

    public static List<String> getPrimaryKeyNames(TableMetadata tm) {
        return tm.getPrimaryKey().stream().map(md -> md.getName().toString()).collect(Collectors.toList());
    }

    public static List<Schema> getPrimaryKeySchemas(TableMetadata tm) {
        return tm.getPrimaryKey().stream()
                .map(ColumnMetadata::getType)
                .map(type -> CassandraTypeDeserializer.getSchemaBuilder(type).build())
                .collect(Collectors.toList());
    }

    public static List<Schema> getPrimaryKeySchemas(List<DataType> dataTypes) {
        return dataTypes.stream()
                .map(type -> CassandraTypeDeserializer.getSchemaBuilder(type).build())
                .collect(Collectors.toList());
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

    /**
     * Get the schema of an inner field based on the field name
     * @param fieldName the name of the field in the schema
     * @param schema the schema where the field resides in
     * @return Schema
     */
    public static Schema getFieldSchema(String fieldName, Schema schema) {
        if (schema.type().equals(Schema.Type.STRUCT)) {
            return schema.field(fieldName).schema();
        }
        throw new CassandraConnectorSchemaException("Only STRUCT type is supported for this method, but encountered " + schema.type());
    }
}
