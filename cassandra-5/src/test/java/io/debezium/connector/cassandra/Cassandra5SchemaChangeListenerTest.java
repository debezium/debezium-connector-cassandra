/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

class Cassandra5SchemaChangeListenerTest {

    private static final CqlIdentifier CDC = CqlIdentifier.fromInternal("cdc");

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
    void canMirrorToOfflineSchemaIsFalseWhenKeyspaceMetadataAbsent() {
        // When the keyspace is not yet in the embedded Schema.instance, getKeyspaceMetadata(...) is null.
        // Before the fix, onTableCreated then called Keyspace.openWithoutSSTables(...) and threw a NullPointerException.
        assertFalse(Cassandra5SchemaChangeListener.canMirrorToOfflineSchema(null));
    }

    @Test
    void canMirrorToOfflineSchemaIsTrueWhenKeyspaceMetadataPresent() {
        assertTrue(Cassandra5SchemaChangeListener.canMirrorToOfflineSchema(
                mock(org.apache.cassandra.schema.KeyspaceMetadata.class)));
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
