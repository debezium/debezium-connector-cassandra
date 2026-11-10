/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import io.debezium.connector.cassandra.KeyspaceTable;
import io.debezium.connector.cassandra.SchemaHolder;
import io.debezium.doc.FixFor;

class DseSchemaChangeListenerTest {

    private static final CqlIdentifier CDC = CqlIdentifier.fromInternal("cdc");
    private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

    @Test
    void isCdcEnabledReturnsFalseWhenCdcOptionIsAbsent() {
        // A table created without CDC has no "cdc" option, so getOptions().get("cdc") is null.
        // Before the fix, onTableCreated/onTableDropped/onTableUpdated called null.toString()
        // and threw a NullPointerException.
        assertFalse(DseSchemaChangeListener.isCdcEnabled(mockTable(null)));
    }

    @Test
    void isCdcEnabledReflectsCdcOption() {
        assertTrue(DseSchemaChangeListener.isCdcEnabled(mockTable("true")));
        assertFalse(DseSchemaChangeListener.isCdcEnabled(mockTable("false")));
    }

    @Test
    @FixFor("debezium/dbz#3036")
    void keyspaceDropContinuesAfterTableWithMissingCdcOption() {
        final var schemaHolder = mock(SchemaHolder.class);
        final var listener = new DseSchemaChangeListener("server", null, schemaHolder);

        final var unmanagedTable = mockTable("unmanaged", null);
        final var managedTable = mockTable("managed", "true");
        final Map<CqlIdentifier, TableMetadata> tables = new LinkedHashMap<>();
        tables.put(unmanagedTable.getName(), unmanagedTable);
        tables.put(managedTable.getName(), managedTable);

        final var keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getName()).thenReturn(KEYSPACE);
        when(keyspace.getTables()).thenReturn(tables);

        listener.onKeyspaceDropped(keyspace);

        verify(schemaHolder).removeTableSchema(new KeyspaceTable(KEYSPACE.asInternal(), managedTable.getName().asInternal()));
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
