/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

/**
 * Row-level data about the source event. Contains a map where the key is the table column
 * name and the value is the {@link CellData}.
 */
public class RowData implements KafkaRecord {
    private final Map<String, CellData> cellMap = new LinkedHashMap<>();

    private Object start = null;
    private Object end = null;

    public void addStart(Object start) {
        this.start = start;
    }

    public void addEnd(Object end) {
        this.end = end;
    }

    public Object getStart() {
        return start;
    }

    public Object getEnd() {
        return end;
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
            if (field.name().equals(".range_start") && start != null) {
                struct.put(field.name(), start);
            }
            else if (field.name().equals(".range_end") && end != null) {
                struct.put(field.name(), end);
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
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(Record.AFTER);

        for (int i = 0; i < columnNames.size(); i++) {
            Schema valueSchema = CassandraTypeDeserializer.getSchemaBuilder(CassandraTypeConverter.convert(columnsTypes.get(i))).build();
            String columnName = columnNames.get(i);
            Schema optionalCellSchema = CellData.cellSchema(columnName, valueSchema, true);
            if (optionalCellSchema != null) {
                schemaBuilder.field(columnName, optionalCellSchema);
            }
        }

        schemaBuilder.field(".range_start", Schema.OPTIONAL_STRING_SCHEMA);
        schemaBuilder.field(".range_end", Schema.OPTIONAL_STRING_SCHEMA);

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
