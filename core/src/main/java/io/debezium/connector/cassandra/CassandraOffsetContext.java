/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

public class CassandraOffsetContext implements OffsetContext {

    private final Map<String, Object> offset;

    private CassandraOffsetContext(Map<String, Object> offset) {
        this.offset = offset;
    }

    @Override
    public Map<String, ?> getOffset() {
        return new HashMap<>(offset);
    }

    @Override
    public Schema getSourceInfoSchema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Struct getSourceInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSnapshotRunning() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preSnapshotStart() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preSnapshotCompletion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void postSnapshotCompletion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext getTransactionContext() {
        throw new UnsupportedOperationException();
    }

    public void putOffset(String table, boolean isSnapshot, String offsetPosition) {
        offset.put(key(table, isSnapshot), offsetPosition);
    }

    public String getOffset(String sourceTable, boolean isSnapshot) {
        return (String) offset.get(key(sourceTable, isSnapshot));
    }

    private String key(String table, boolean isSnapshot) {
        return table + "." + (isSnapshot ? "snapshot" : "commitlog");
    }

    public static class Loader implements OffsetContext.Loader<CassandraOffsetContext> {

        @Override
        public CassandraOffsetContext load(Map<String, ?> offset) {
            return new CassandraOffsetContext(new HashMap<>(offset));
        }
    }
}
