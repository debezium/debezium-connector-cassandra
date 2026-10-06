/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;

import io.debezium.doc.FixFor;

class Cassandra5SchemaChangeListenerTest {

    private static final CqlIdentifier CDC = CqlIdentifier.fromInternal("cdc");
    private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

    @Test
    void isCdcEnabledReturnsFalseWhenCdcOptionIsAbsent() {
        // A table created without CDC has no "cdc" option, so getOptions().get("cdc") is null.
        // Before the fix, onTableUpdated called null.toString() here and threw a NullPointerException.
        assertFalse(Cassandra5SchemaChangeListener.isCdcEnabled(mockTable(null)));
    }

    @Test
    void isCdcEnabledReflectsCdcOption() {
        assertTrue(Cassandra5SchemaChangeListener.isCdcEnabled(mockTable("true")));
        assertFalse(Cassandra5SchemaChangeListener.isCdcEnabled(mockTable("false")));
    }

    @Test
    @FixFor("debezium/dbz#2736")
    void resolveKeyspaceFromSessionReturnsEmptyWhenSessionIsNull() {
        // No session captured yet, so the keyspace cannot be resolved to register before mirroring.
        assertFalse(Cassandra5SchemaChangeListener.resolveKeyspaceFromSession(null, KEYSPACE).isPresent());
    }

    @Test
    @FixFor("debezium/dbz#2736")
    void resolveKeyspaceFromSessionReturnsKeyspaceWhenPresent() {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        Session session = mockSession(KEYSPACE, keyspace);
        Optional<KeyspaceMetadata> resolved = Cassandra5SchemaChangeListener.resolveKeyspaceFromSession(session, KEYSPACE);
        assertTrue(resolved.isPresent());
        assertSame(keyspace, resolved.get());
    }

    @Test
    @FixFor("debezium/dbz#2736")
    void resolveKeyspaceFromSessionReturnsEmptyWhenKeyspaceAbsent() {
        Session session = mockSession(KEYSPACE, null);
        assertFalse(Cassandra5SchemaChangeListener.resolveKeyspaceFromSession(session, KEYSPACE).isPresent());
    }

    private Session mockSession(CqlIdentifier keyspace, KeyspaceMetadata keyspaceMetadata) {
        Session session = mock(Session.class);
        Metadata metadata = mock(Metadata.class);
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(keyspace)).thenReturn(Optional.ofNullable(keyspaceMetadata));
        return session;
    }

    private TableMetadata mockTable(String cdcValue) {
        TableMetadata table = mock(TableMetadata.class);
        Map<CqlIdentifier, Object> options = new HashMap<>();
        if (cdcValue != null) {
            options.put(CDC, cdcValue);
        }
        when(table.getOptions()).thenReturn(options);
        return table;
    }
}
