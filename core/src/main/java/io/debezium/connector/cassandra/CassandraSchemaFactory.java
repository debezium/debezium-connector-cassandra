/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.CLUSTERING;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.PARTITION;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.RangeData.RANGE_END_NAME;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.RangeData.RANGE_START_NAME;
import static java.lang.String.format;
import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

import io.debezium.DebeziumException;
import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.schema.SchemaFactory;

public class CassandraSchemaFactory extends SchemaFactory {

    public CassandraSchemaFactory() {
        super();
    }

    private static final CassandraSchemaFactory cassandraSchemaFactoryObject = new CassandraSchemaFactory();

    public static CassandraSchemaFactory get() {
        return cassandraSchemaFactoryObject;
    }

    public RowData rowData() {
        return new RowData();
    }

    public CellData cellData(String name, Object value, Object deletionTs, CellData.ColumnType columnType) {
        return new CellData(name, value, deletionTs, columnType);
    }

    public RangeData rangeData(String name, String method, Map<String, String> values) {
        return new RangeData(name, method, values);
    }

    /**
     * Row-level data about the source event. Contains a map where the key is the table column
     * name and the value is the {@link CellData}.
     */
    public static class RowData implements KafkaRecord {
        private final Map<String, CellData> cellMap = new LinkedHashMap<>();

        private RangeData startRange = null;
        private RangeData endRange = null;

        private RowData() {
        }

        public void addStartRange(RangeData startRange) {
            this.startRange = startRange;
        }

        public void addEndRange(RangeData endRange) {
            this.endRange = endRange;
        }

        public RangeData getStartRange() {
            return startRange;
        }

        public RangeData getEndRange() {
            return endRange;
        }

        public void addCell(CellData cellData) {
            this.cellMap.put(cellData.name, cellData);
        }

        public void removeCell(String columnName) {
            if (hasCell(columnName)) {
                cellMap.remove(columnName);
            }
        }

        public boolean hasCell(String columnName) {
            return cellMap.containsKey(columnName);
        }

        public boolean hasAnyCell() {
            return !cellMap.isEmpty();
        }

        @Override
        public Struct record(Schema schema) {
            Struct struct = new Struct(schema);
            for (Field field : schema.fields()) {
                Schema cellSchema = KeyValueSchema.getFieldSchema(field.name(), schema);
                if (field.name().equals(RANGE_START_NAME) && startRange != null) {
                    struct.put(field.name(), startRange.record(cellSchema));
                }
                else if (field.name().equals(RANGE_END_NAME) && endRange != null) {
                    struct.put(field.name(), endRange.record(cellSchema));
                }
                else {
                    CellData cellData = cellMap.get(field.name());
                    if (cellData != null) {
                        struct.put(field.name(), cellData.record(cellSchema));
                    }
                }
            }
            return struct;
        }

        public RowData copy() {
            RowData copy = new RowData();
            for (CellData cellData : cellMap.values()) {
                copy.addCell(cellData);
            }
            return copy;
        }

        /**
         * Assemble the Kafka connect {@link Schema} for the "after" field of the change event
         * based on the Cassandra table schema.
         *
         * @param tm metadata of a table that contains the Cassandra table schema
         * @return a schema for the "after" field of a change event
         */
        static Schema rowSchema(TableMetadata tm) {
            List<String> columnNames = new ArrayList<>();
            List<DataType> columnTypes = new ArrayList<>();

            for (ColumnMetadata cm : tm.getColumns().values()) {
                columnNames.add(cm.getName().toString());
                columnTypes.add(cm.getType());
            }

            return rowSchema(columnNames, columnTypes);
        }

        static Schema rowSchema(List<String> columnNames, List<DataType> columnsTypes) {
            SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(Record.AFTER).version(1);

            for (int i = 0; i < columnNames.size(); i++) {
                Schema valueSchema = CassandraTypeDeserializer.getSchemaBuilder(
                        CassandraTypeConverter.convert(columnsTypes.get(i))).build();
                String columnName = columnNames.get(i);
                Schema optionalCellSchema = CellData.cellSchema(columnName, valueSchema, true);
                if (optionalCellSchema != null) {
                    schemaBuilder.field(columnName, optionalCellSchema);
                }
            }

            schemaBuilder.field(RANGE_START_NAME, RangeData.rangeSchema(RANGE_START_NAME));
            schemaBuilder.field(RANGE_END_NAME, RangeData.rangeSchema(RANGE_END_NAME));

            return schemaBuilder.build();
        }

        List<CellData> getPrimary() {
            return this.cellMap.values().stream().filter(CellData::isPrimary).collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return this.cellMap.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RowData rowData = (RowData) o;
            return Objects.equals(cellMap, rowData.cellMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cellMap);
        }
    }

    /**
     * Cell-level data about the source event. Each cell contains the name, value and
     * type of column in a Cassandra table.
     */
    public static class CellData implements KafkaRecord {

        /**
         * The type of column in a Cassandra table
         */
        public enum ColumnType {
            /**
             * A partition column is responsible for data distribution across nodes for this table.
             * Every Cassandra table must have at least one partition column.
             */
            PARTITION,

            /**
             * A clustering column is used to specify the order that the data is arranged inside the partition.
             * A Cassandra table may not have any clustering column,
             */
            CLUSTERING,

            /**
             * A regular column is a column that is not a partition or a clustering column.
             */
            REGULAR
        }

        public static final String CELL_VALUE_KEY = "value";
        public static final String CELL_DELETION_TS_KEY = "deletion_ts";
        public static final String CELL_SET_KEY = "set";

        public final String name;
        public final Object value;
        public final Object deletionTs;
        public final ColumnType columnType;

        private CellData(String name, Object value, Object deletionTs, ColumnType columnType) {
            this.name = name;
            this.value = value;
            this.deletionTs = deletionTs;
            this.columnType = columnType;
        }

        public boolean isPrimary() {
            return columnType == PARTITION || columnType == CLUSTERING;
        }

        @Override
        public Struct record(Schema schema) {
            try {
                return new Struct(schema)
                        .put(CELL_DELETION_TS_KEY, deletionTs)
                        .put(CELL_SET_KEY, true)
                        .put(CELL_VALUE_KEY, value);
            }
            catch (DataException e) {
                throw new DebeziumException(format("Failed to record Cell. Name: %s, Schema: %s, Value: %s",
                        name, schema.toString(), value), e);
            }
        }

        static Schema cellSchema(String columnName, Schema columnSchema, boolean optional) {
            if (columnSchema == null) {
                return null;
            }

            SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                    .name(columnName)
                    .version(1)
                    .field(CELL_VALUE_KEY, columnSchema)
                    .field(CELL_DELETION_TS_KEY, OPTIONAL_INT64_SCHEMA)
                    .field(CELL_SET_KEY, BOOLEAN_SCHEMA);
            if (optional) {
                schemaBuilder.optional();
            }
            return schemaBuilder.build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CellData that = (CellData) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(value, that.value)
                    && deletionTs == that.deletionTs
                    && columnType == that.columnType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value, deletionTs, columnType);
        }

        @Override
        public String toString() {
            return "{"
                    + "name=" + name
                    + ", value=" + value
                    + ", deletionTs=" + deletionTs
                    + ", type=" + columnType.name()
                    + '}';
        }
    }

    public static class RangeData implements KafkaRecord {
        public static final String RANGE_START_NAME = "_range_start";
        public static final String RANGE_END_NAME = "_range_end";
        public static final String RANGE_METHOD_FIELD_NAME = "method";
        public static final String RANGE_VALUES_FIELD_NAME = "values";
        public static final String RANGE_TOMBSTONE_VALUES_MAP_NAME = "range_tombstone_values_map";

        public final String name;
        public final String method;
        public final Map<String, String> values = new HashMap<>();

        private RangeData(String name, String method, Map<String, String> values) {
            if (name == null) {
                throw new IllegalArgumentException("Name of range can not be null!");
            }

            if (!name.equals(RANGE_START_NAME) && !name.equals(RANGE_END_NAME)) {
                throw new IllegalArgumentException(format("Value of name parameter has to be either %s or %s",
                        RANGE_START_NAME, RANGE_END_NAME));
            }

            this.name = name;
            this.method = method;
            if (values != null) {
                this.values.putAll(values);
            }
        }

        public static RangeData start(String method, Map<String, String> values) {
            return CassandraSchemaFactory.get().rangeData(RANGE_START_NAME, method, values);
        }

        public static RangeData end(String method, Map<String, String> values) {
            return CassandraSchemaFactory.get().rangeData(RANGE_END_NAME, method, values);
        }

        @Override
        public Struct record(Schema schema) {
            try {
                return new Struct(schema)
                        .put(RANGE_METHOD_FIELD_NAME, method)
                        .put(RANGE_VALUES_FIELD_NAME, values);
            }
            catch (DataException e) {
                throw new DebeziumException(format("Failed to record Range. Name: %s, Schema: %s, Method: %s Value: %s",
                        name, schema.toString(), method, values), e);
            }
        }

        static Schema rangeSchema(String name) {
            Schema map = SchemaBuilder.map(STRING_SCHEMA, STRING_SCHEMA)
                    .name(RANGE_TOMBSTONE_VALUES_MAP_NAME)
                    .version(1)
                    .build();

            return SchemaBuilder.struct()
                    .name(name)
                    .version(1)
                    .field(RANGE_METHOD_FIELD_NAME, STRING_SCHEMA)
                    .field(RANGE_VALUES_FIELD_NAME, map)
                    .optional()
                    .build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RangeData that = (RangeData) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(method, that.method)
                    && values.equals(that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, method, values);
        }

        @Override
        public String toString() {
            return "{"
                    + "name=" + name
                    + ", method=" + method
                    + ", values=" + values
                    + '}';
        }
    }
}
