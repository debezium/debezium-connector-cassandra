/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class Cassandra4OfflineKeyspaceUpdateTest {

    @Test
    @FixFor("debezium/dbz#2792")
    void offlineKeyspaceUpdatePreservesExistingReplicationParams() {
        // The keyspace already mirrored into the embedded schema (e.g. RF=1).
        KeyspaceMetadata existing = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1));

        KeyspaceMetadata result = Cassandra4SchemaChangeListener.offlineKeyspaceUpdate(existing);

        // The metadata to re-apply must keep the existing replication params, regardless of what
        // replication factor the driver update reports, so the replication-change guard stays false.
        assertEquals(existing.params.replication, result.params.replication,
                "offlineKeyspaceUpdate must not change the embedded keyspace's replication params");
        assertEquals(existing.name, result.name);
    }

    @Test
    @FixFor("debezium/dbz#2792")
    void offlineKeyspaceUpdateDoesNotAdoptADifferentReplicationFactor() {
        // Guards against a regression where the driver's (differing) replication factor is applied.
        KeyspaceMetadata existing = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));

        KeyspaceMetadata result = Cassandra4SchemaChangeListener.offlineKeyspaceUpdate(existing);

        assertSame(existing.params, result.params,
                "offlineKeyspaceUpdate must reuse the existing params instance, never build new ones");
    }
}
