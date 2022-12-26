/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.session.Session;

import io.debezium.connector.SourceInfoStructMaker;

public class Cassandra4SchemaChangeListener extends AbstractSchemaChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4SchemaChangeListener.class);

    public Cassandra4SchemaChangeListener(String kafkaTopicPrefix,
                                          SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker,
                                          SchemaHolder schemaHolder) {
        super(kafkaTopicPrefix, sourceInfoStructMaker, schemaHolder);
    }

    @Override
    public void onSessionReady(Session session) {
        LOGGER.info("Initializing SchemaHolder ...");
        List<com.datastax.oss.driver.api.core.metadata.schema.TableMetadata> cdcEnabledTableMetadataList = getCdcEnabledTableMetadataList(session);
        for (com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tm : cdcEnabledTableMetadataList) {
            schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(tm), getKeyValueSchema(tm));
            onKeyspaceCreated(session.getMetadata().getKeyspace(tm.getKeyspace().toString()).get());
            onTableCreated(tm);
        }

        Set<String> cdcEnabledEntities = schemaHolder.getCdcEnabledTableMetadataSet()
                .stream()
                .map(tmd -> tmd.describe(true))
                .collect(Collectors.toSet());

        LOGGER.info("CDC enabled entities: {}", cdcEnabledEntities);
        LOGGER.info("Initialized SchemaHolder.");
    }

    @Override
    public void onKeyspaceCreated(final KeyspaceMetadata keyspaceMetadata) {
        try {
            org.apache.cassandra.schema.KeyspaceMetadata existingKMD = Schema.instance.getKeyspaceMetadata(keyspaceMetadata.getName().asInternal());
            if (existingKMD != null) {
                return;
            }

            org.apache.cassandra.schema.KeyspaceMetadata newKMD = org.apache.cassandra.schema.KeyspaceMetadata.create(
                    keyspaceMetadata.getName().toString(),
                    KeyspaceParams.create(keyspaceMetadata.isDurableWrites(),
                            keyspaceMetadata.getReplication()));

            Schema.instance.load(newKMD);
            Keyspace.openWithoutSSTables(keyspaceMetadata.getName().toString());
            LOGGER.info("Added keyspace [{}] to schema instance.", keyspaceMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while adding the keyspace {} to schema instance.", keyspaceMetadata.getName(), e);
        }
    }

    @Override
    public void onKeyspaceUpdated(final KeyspaceMetadata current, final KeyspaceMetadata previous) {
        try {
            Schema.instance.load(org.apache.cassandra.schema.KeyspaceMetadata.create(
                    current.getName().asInternal(),
                    KeyspaceParams.create(current.isDurableWrites(),
                            current.getReplication())));
            LOGGER.info("Updated keyspace [{}] in schema instance.", current.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while updating the keyspace {} in schema instance.", current.getName(), e);
        }
    }

    @Override
    public void onKeyspaceDropped(final KeyspaceMetadata keyspaceMetadata) {
        try {
            for (Map.Entry<CqlIdentifier, com.datastax.oss.driver.api.core.metadata.schema.TableMetadata> entries : keyspaceMetadata.getTables().entrySet()) {
                onTableDropped(entries.getValue());
            }
            Schema.instance.removeKeyspaceInstance(keyspaceMetadata.getName().toString());
            LOGGER.info("Removed keyspace [{}] from schema instance.", keyspaceMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while removing the keyspace {} from schema instance.", keyspaceMetadata.getName(), e);
        }
    }

    @Override
    public void onTableCreated(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tableMetadata) {
        Object cdc = tableMetadata.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean cdcEnabled = cdc.toString().equals("true");

        if (cdcEnabled) {
            schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(tableMetadata), getKeyValueSchema(tableMetadata));
        }
        try {
            LOGGER.info("Table {}.{} detected to be added!", tableMetadata.getKeyspace(), tableMetadata.getName());

            UUID uuid = tableMetadata.getId().get();

            org.apache.cassandra.schema.TableMetadata metadata = CreateTableStatement.parse(tableMetadata.describe(true),
                    tableMetadata.getKeyspace().toString())
                    .id(TableId.fromUUID(uuid))
                    .build();

            final Keyspace keyspace = Keyspace.openWithoutSSTables(tableMetadata.getKeyspace().asInternal());
            if (keyspace.hasColumnFamilyStore(metadata.id)) {
                return;
            }
            keyspace.initCfCustom(ColumnFamilyStore.createColumnFamilyStore(keyspace,
                    metadata.name,
                    TableMetadataRef.forOfflineTools(metadata),
                    new Directories(metadata),
                    false,
                    false,
                    true));

            final org.apache.cassandra.schema.KeyspaceMetadata current = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
            if (current == null) {
                LOGGER.warn("Keyspace {} doesn't exist", metadata.keyspace);
                return;
            }
            if (current.tables.get(tableMetadata.getName().toString()).isPresent()) {
                LOGGER.debug("Table {}.{} is already added!", tableMetadata.getKeyspace(), tableMetadata.getName());
                return;
            }
            org.apache.cassandra.schema.KeyspaceMetadata transformed = current.withSwapped(current.tables.with(metadata));
            Schema.instance.load(transformed);
            LOGGER.info("Added table [{}] to schema instance.", tableMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while adding table {}.{} to schema instance.", tableMetadata.getKeyspace(), tableMetadata.getName(), e);
        }
    }

    @Override
    public void onTableDropped(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tableMetadata) {
        Object cdc = tableMetadata.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean cdcEnabled = cdc.toString().equals("true");

        if (cdcEnabled) {
            schemaHolder.removeTableSchema(new KeyspaceTable(tableMetadata));
        }

        try {
            final String ksName = tableMetadata.getKeyspace().toString();
            final String tableName = tableMetadata.getName().toString();
            LOGGER.debug("Table {}.{} detected to be removed!", ksName, tableName);
            final org.apache.cassandra.schema.KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
            if (oldKsm == null) {
                LOGGER.warn("KeyspaceMetadata for keyspace {} is not found!", ksName);
                return;
            }

            TableMetadata metadata = Schema.instance.getTableMetadata(TableId.fromUUID(tableMetadata.getId().get()));
            if (metadata == null) {
                LOGGER.warn("Metadata for ColumnFamilyStore for {}.{} is not found!", ksName, tableName);
                return;
            }

            Keyspace instance = Schema.instance.getKeyspaceInstance(metadata.keyspace);
            if (instance == null) {
                LOGGER.warn("Keyspace instance for ColumnFamilyStore for {}.{} is not found!", ksName, tableName);
                return;
            }

            final ColumnFamilyStore cfs = instance.hasColumnFamilyStore(metadata.id)
                    ? instance.getColumnFamilyStore(metadata.id)
                    : null;

            if (cfs == null) {
                LOGGER.warn("ColumnFamilyStore for {}.{} is not found!", ksName, tableName);
                return;
            }
            // make sure all the indexes are dropped, or else.
            cfs.indexManager.markAllIndexesRemoved();
            // reinitialize the keyspace.
            final Optional<TableMetadata> cfm = oldKsm.tables.get(tableName);

            if (cfm.isPresent()) {
                final org.apache.cassandra.schema.KeyspaceMetadata newKsm = oldKsm.withSwapped(oldKsm.tables.without(tableName));
                Schema.instance.load(newKsm);
                LOGGER.info("Removed table [{}] from schema instance.", tableMetadata.describe(true));
            }
            else {
                LOGGER.warn("Table {}.{} is not present in old keyspace meta data!", ksName, tableName);
            }

        }
        catch (Exception e) {
            LOGGER.warn("Error happened while removing table {}.{} from schema instance.", tableMetadata.getKeyspace(), tableMetadata.getName(), e);
        }
    }

    @Override
    public void onTableUpdated(final com.datastax.oss.driver.api.core.metadata.schema.TableMetadata newTableMetadata,
                               final com.datastax.oss.driver.api.core.metadata.schema.TableMetadata oldTableMetaData) {
        Object newCdcObject = newTableMetadata.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean newCdc = newCdcObject.toString().equals("true");
        Object oldCdcObject = oldTableMetaData.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean oldCdc = oldCdcObject.toString().equals("true");

        if (newCdc) {
            // if it was cdc before and now it is too, add it, because its schema might change
            // however if it is CDC-enabled but it was not, update it in schema too because its cdc flag has changed
            // this basically means we add / update every time if new has cdc flag equals to true
            schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(newTableMetadata), getKeyValueSchema(newTableMetadata));
        }
        else if (oldCdc) {
            // if new table is not on cdc anymore, and we see the old one was, remove it
            schemaHolder.removeTableSchema(new KeyspaceTable(newTableMetadata));
        }
        try {
            LOGGER.debug("Detected alternation in schema of {}.{} (previous cdc = {}, current cdc = {})",
                    newTableMetadata.getKeyspace(),
                    newTableMetadata.getName(),
                    oldCdc,
                    newCdc);
            // else if it was not cdc before nor now, do nothing with schema holder
            // but add it to Cassandra for subsequent deserialization path in every case

            UUID uuid = UUID.nameUUIDFromBytes(ArrayUtils.addAll(newTableMetadata.getKeyspace().toString().getBytes(),
                    newTableMetadata.getName().toString().getBytes()));

            org.apache.cassandra.schema.TableMetadata metadata = CreateTableStatement.parse(newTableMetadata.describe(true),
                    newTableMetadata.getKeyspace().toString())
                    .id(TableId.fromUUID(uuid))
                    .build();

            org.apache.cassandra.schema.KeyspaceMetadata current = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
            if (current != null) {
                Schema.instance.load(current.withSwapped(current.tables.withSwapped(metadata)));
            }
            LOGGER.info("Updated table [{}] in schema instance.", newTableMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while reacting on changed table {}.{} in schema instance.", newTableMetadata.getKeyspace(), newTableMetadata.getName(), e);
        }
    }
}
