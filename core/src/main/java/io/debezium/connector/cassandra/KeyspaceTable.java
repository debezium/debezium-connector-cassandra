/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * The KeyspaceTable uniquely identifies each table in the Cassandra cluster
 */
public class KeyspaceTable implements DataCollectionId {
    public final String keyspace;
    public final String table;

    public KeyspaceTable(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public KeyspaceTable(TableMetadata tableMetadata) {
        this.keyspace = tableMetadata.getKeyspace().toString();
        this.table = tableMetadata.getName().toString();
    }

    public String name() {
        return keyspace + "." + table;
    }

    public List<String> parts() {
        return Collect.arrayListOf(keyspace, table);
    }

    public List<String> databaseParts() {
        return Collections.emptyList();
    }

    public List<String> schemaParts() {
        return Collect.arrayListOf(keyspace, table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyspaceTable that = (KeyspaceTable) o;
        return keyspace.equals(that.keyspace) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, table);
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public String identifier() {
        return keyspace + "." + table;
    }
}
