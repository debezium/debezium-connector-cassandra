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

class Cassandra4SchemaChangeListenerTest {

    private static final CqlIdentifier CDC = CqlIdentifier.fromInternal("cdc");

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
