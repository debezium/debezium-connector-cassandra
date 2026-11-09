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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;

import io.debezium.doc.FixFor;

class Cassandra4SchemaChangeListenerTest {

    private static final CqlIdentifier CDC = CqlIdentifier.fromInternal("cdc");
    private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

    @Test
    void isCdcEnabledReturnsFalseWhenCdcOptionIsAbsent() {
        // A table created without CDC has no "cdc" option, so getOptions().get("cdc") is null.
        assertFalse(Cassandra4SchemaChangeListener.isCdcEnabled(mockTable(null)));
    }

    @Test
    void isCdcEnabledReflectsCdcOption() {
        assertTrue(Cassandra4SchemaChangeListener.isCdcEnabled(mockTable("true")));
        assertFalse(Cassandra4SchemaChangeListener.isCdcEnabled(mockTable("false")));
    }

    @Test
    @FixFor("debezium/dbz#3036")
    void keyspaceDropContinuesAfterTableWithMissingCdcOption() {
        SchemaHolder schemaHolder = mock(SchemaHolder.class);
        Cassandra4SchemaChangeListener listener = new Cassandra4SchemaChangeListener("server", null, schemaHolder);

        TableMetadata unmanagedTable = mockTable("unmanaged", null);
        TableMetadata managedTable = mockTable("managed", "true");
        Map<CqlIdentifier, TableMetadata> tables = new LinkedHashMap<>();
        tables.put(unmanagedTable.getName(), unmanagedTable);
        tables.put(managedTable.getName(), managedTable);

        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getName()).thenReturn(KEYSPACE);
        when(keyspace.getTables()).thenReturn(tables);

        listener.onKeyspaceDropped(keyspace);

        verify(schemaHolder).removeTableSchema(new KeyspaceTable(KEYSPACE.asInternal(), managedTable.getName().asInternal()));
    }

    @Test
    @FixFor("debezium/dbz#2736")
    void resolveKeyspaceFromSessionReturnsEmptyWhenSessionIsNull() {
        // No session captured yet, so the keyspace cannot be resolved to register before mirroring.
        assertFalse(Cassandra4SchemaChangeListener.resolveKeyspaceFromSession(null, KEYSPACE).isPresent());
    }

    @Test
    @FixFor("debezium/dbz#2736")
    void resolveKeyspaceFromSessionReturnsKeyspaceWhenPresent() {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        Session session = mockSession(KEYSPACE, keyspace);
        Optional<KeyspaceMetadata> resolved = Cassandra4SchemaChangeListener.resolveKeyspaceFromSession(session, KEYSPACE);
        assertTrue(resolved.isPresent());
        assertSame(keyspace, resolved.get());
    }

    @Test
    @FixFor("debezium/dbz#2736")
    void resolveKeyspaceFromSessionReturnsEmptyWhenKeyspaceAbsent() {
        Session session = mockSession(KEYSPACE, null);
        assertFalse(Cassandra4SchemaChangeListener.resolveKeyspaceFromSession(session, KEYSPACE).isPresent());
    }

    private Session mockSession(CqlIdentifier keyspace, KeyspaceMetadata keyspaceMetadata) {
        Session session = mock(Session.class);
        Metadata metadata = mock(Metadata.class);
        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(keyspace)).thenReturn(Optional.ofNullable(keyspaceMetadata));
        return session;
    }

    private TableMetadata mockTable(String cdcValue) {
        return mockTable("table", cdcValue);
    }

    private TableMetadata mockTable(String name, String cdcValue) {
        TableMetadata table = mock(TableMetadata.class);
        Map<CqlIdentifier, Object> options = new HashMap<>();
        if (cdcValue != null) {
            options.put(CDC, cdcValue);
        }
        when(table.getOptions()).thenReturn(options);
        when(table.getKeyspace()).thenReturn(KEYSPACE);
        when(table.getName()).thenReturn(CqlIdentifier.fromInternal(name));
        return table;
    }
}
