/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.function.BlockingConsumer;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
public class RecordMaker {
    private final boolean emitTombstoneOnDelete;
    private final Filters filters;
    private final CassandraConnectorConfig config;

    public RecordMaker(boolean emitTombstoneOnDelete, Filters filters, CassandraConnectorConfig config) {
        this.emitTombstoneOnDelete = emitTombstoneOnDelete;
        this.filters = filters;
        this.config = config;
    }

    public void insert(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                       Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                       boolean markOffset, BlockingConsumer<Record> consumer) {
        createRecord(cluster, offsetPosition, keyspaceTable, snapshot, tsMicro,
                data, keySchema, valueSchema, markOffset, consumer, Record.Operation.INSERT);
    }

    public void update(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                       Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                       boolean markOffset, BlockingConsumer<Record> consumer) {
        createRecord(cluster, offsetPosition, keyspaceTable, snapshot, tsMicro,
                data, keySchema, valueSchema, markOffset, consumer, Record.Operation.UPDATE);
    }

    public void delete(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                       Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                       boolean markOffset, BlockingConsumer<Record> consumer) {
        createRecord(cluster, offsetPosition, keyspaceTable, snapshot, tsMicro,
                data, keySchema, valueSchema, markOffset, consumer, Record.Operation.DELETE);
    }

    private void createRecord(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                              Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                              boolean markOffset, BlockingConsumer<Record> consumer, Record.Operation operation) {
        FieldFilterSelector.FieldFilter fieldFilter = filters.getFieldFilter(keyspaceTable);
        RowData filteredData;
        switch (operation) {
            case INSERT:
            case UPDATE:
                filteredData = fieldFilter.apply(data);
                break;
            case DELETE:
            default:
                filteredData = data;
                break;
        }

        SourceInfo source = new SourceInfo(config, cluster, offsetPosition, keyspaceTable, snapshot, tsMicro);
        ChangeRecord record = new ChangeRecord(source, filteredData, keySchema, valueSchema, operation, markOffset);
        try {
            consumer.accept(record);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            throw new CassandraConnectorTaskException(String.format(
                    "Enqueuing has been interrupted while enqueuing Change Event %s", record.toString()), e);
        }
        if (operation == Record.Operation.DELETE && emitTombstoneOnDelete) {
            // generate kafka tombstone event
            TombstoneRecord tombstoneRecord = new TombstoneRecord(source, filteredData, keySchema);
            try {
                consumer.accept(tombstoneRecord);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                throw new CassandraConnectorTaskException(String.format(
                        "Enqueuing has been interrupted while enqueuing Tombstone Event %s", record.toString()), e);
            }
        }
    }
}
