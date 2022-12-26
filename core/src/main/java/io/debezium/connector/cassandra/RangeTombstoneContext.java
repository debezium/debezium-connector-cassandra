/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;

/**
 * Range tombstone which comes in PartitionUpdate is consisting of 2 RangeTombstoneBoundMarker's
 * which are logically related to each other as the first one is for "start" and the second one for "end" marker.
 * We need to group these markers together into one event otherwise it does not make too much sense to
 * send an event with a start marker without an end marker because it would be problematic to decouple it on the
 * consumer's side.
 */
public class RangeTombstoneContext<T> {
    public Map<T, RowData> map = new ConcurrentHashMap<>();
    private final CassandraSchemaFactory factory = CassandraSchemaFactory.get();

    public static boolean isComplete(RowData rowData) {
        return rowData.getStartRange() != null && rowData.getEndRange() != null;
    }

    public RowData getOrCreate(T metadata) {
        RowData rowData = map.get(metadata);
        if (rowData == null) {
            rowData = factory.rowData();
            map.put(metadata, rowData);
        }
        return rowData;
    }

    public void remove(T metadata) {
        map.remove(metadata);
    }
}
