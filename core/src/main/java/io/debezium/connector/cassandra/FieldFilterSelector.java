/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.ArrayList;
import java.util.List;

import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;

/**
 * This field filter selector is designed to determine the filter for excluding fields from a table.
 */
public class FieldFilterSelector {
    private final List<String> fieldExcludeList;

    FieldFilterSelector(List<String> fieldExcludeList) {
        this.fieldExcludeList = fieldExcludeList;
    }

    public interface FieldFilter {
        RowData apply(RowData rowData);
    }

    /**
     * Returns the field(s) filter for the given table.
     */
    public FieldFilter selectFieldFilter(KeyspaceTable keyspaceTable) {
        List<Field> filteredFields = new ArrayList<>();
        for (String column : fieldExcludeList) {
            Field field = new Field(column);
            if (field.keyspaceTable.equals(keyspaceTable)) {
                filteredFields.add(field);
            }
        }

        if (filteredFields.size() > 0) {
            return rowData -> {
                RowData copy = rowData.copy();
                for (Field field : filteredFields) {
                    if (copy.hasCell(field.column)) {
                        copy.removeCell(field.column);
                    }
                }
                return copy;
            };
        }
        else {
            return rowData -> rowData;
        }
    }

    /**
     * Representation of a fully qualified field, which has a {@link KeyspaceTable}
     * and a field name. Nested and repeated fields are not supported right now.
     */
    private static final class Field {
        final KeyspaceTable keyspaceTable;
        final String column;

        private Field(String fieldExcludeList) {
            if (Strings.isNullOrEmpty(fieldExcludeList)) {
                throw new IllegalArgumentException("Field exclude list entry cannot be null or empty");
            }

            String[] elements = fieldExcludeList.split("\\.", -1);
            if (elements.length != 3) {
                throw new IllegalArgumentException(
                    "Invalid field exclude list format: '" + fieldExcludeList + 
                    "'. Expected format: 'keyspace.table.column'"
                );
            }

            String keyspace = elements[0].trim();
            String table = elements[1].trim();
            String column = elements[2].trim();
            
            if (keyspace.isEmpty() || table.isEmpty() || column.isEmpty()) {
                throw new IllegalArgumentException(
                    "Keyspace, table, and column names cannot be empty in: '" + fieldExcludeList + "'"
                );
            }

            keyspaceTable = new KeyspaceTable(keyspace, table);
            this.column = column;
        }
    }

}
