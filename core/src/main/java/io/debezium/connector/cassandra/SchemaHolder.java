/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public class SchemaHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHolder.class);

    private final ConcurrentMap<KeyspaceTable, KeyValueSchema> tableToKVSchemaMap;

    public SchemaHolder() {
        this.tableToKVSchemaMap = new ConcurrentHashMap<>();
    }

    public KeyValueSchema getKeyValueSchema(KeyspaceTable kst) {
        return tableToKVSchemaMap.getOrDefault(kst, null);
    }

    public Set<TableMetadata> getCdcEnabledTableMetadataSet() {
        return tableToKVSchemaMap.values().stream()
                .map(KeyValueSchema::tableMetadata)
                .collect(Collectors.toSet());
    }

    public void removeTableSchema(KeyspaceTable kst) {
        tableToKVSchemaMap.remove(kst);
        LOGGER.info("Removed the schema for {}.{} from table schema cache.", kst.keyspace, kst.table);
    }

    public void addOrUpdateTableSchema(KeyspaceTable kst, KeyValueSchema kvs) {
        boolean isUpdate = tableToKVSchemaMap.containsKey(kst);
        tableToKVSchemaMap.put(kst, kvs);
        if (isUpdate) {
            LOGGER.info("Updated the schema for {}.{} in table schema cache.", kst.keyspace, kst.table);
        }
        else {
            LOGGER.info("Added the schema for {}.{} to table schema cache.", kst.keyspace, kst.table);
        }
    }
}
