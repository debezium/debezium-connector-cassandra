/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;

import io.debezium.connector.SourceInfoStructMaker;

public class Cassandra3SchemaChangeListener extends AbstractSchemaChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra3SchemaChangeListener.class);

    public Cassandra3SchemaChangeListener(String kafkaTopicPrefix,
                                          SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker,
                                          SchemaHolder schemaHolder) {
        super(kafkaTopicPrefix, sourceInfoStructMaker, schemaHolder);
    }

    @Override
    public void onSessionReady(Session session) {
        LOGGER.info("Initializing SchemaHolder ...");
        List<TableMetadata> cdcEnabledTableMetadataList = getCdcEnabledTableMetadataList(session);
        for (com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tm : cdcEnabledTableMetadataList) {
            schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(tm), new KeyValueSchema(this.kafkaTopicPrefix, tm, this.sourceInfoStructMaker));
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
            Schema.instance.setKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                    keyspaceMetadata.getName().toString(),
                    KeyspaceParams.create(keyspaceMetadata.isDurableWrites(),
                            keyspaceMetadata.getReplication())));
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
            Schema.instance.updateKeyspace(current.getName().toString(), KeyspaceParams.create(current.isDurableWrites(), current.getReplication()));
            LOGGER.info("Updated keyspace [{}] in schema instance.", current.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while updating the keyspace {} in schema instance.", current.getName(), e);
        }
    }

    @Override
    public void onKeyspaceDropped(final KeyspaceMetadata keyspaceMetadata) {
        try {
            Schema.instance.clearKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                    keyspaceMetadata.getName().toString(),
                    KeyspaceParams.create(keyspaceMetadata.isDurableWrites(),
                            keyspaceMetadata.getReplication())));
            LOGGER.info("Removed keyspace [{}] from schema instance.", keyspaceMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while removing the keyspace {} from schema instance.", keyspaceMetadata.getName(), e);
        }
    }

    @Override
    public void onTableCreated(final TableMetadata tableMetadata) {
        Object cdc = tableMetadata.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean cdcEnabled = cdc.toString().equals("true");

        if (cdcEnabled) {
            schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(tableMetadata),
                    new KeyValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker));
        }
        try {
            LOGGER.debug("Table {}.{} detected to be added!", tableMetadata.getKeyspace(), tableMetadata.getName());
            final CFMetaData rawCFMetaData = CFMetaData.compile(tableMetadata.describe(true), tableMetadata.getKeyspace().toString());
            // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
            final CFMetaData newCFMetaData = rawCFMetaData.copy(tableMetadata.getId().get());

            final Keyspace keyspace = Keyspace.openWithoutSSTables(tableMetadata.getKeyspace().asInternal());
            if (keyspace.hasColumnFamilyStore(newCFMetaData.cfId)) {
                return;
            }
            keyspace.initCfCustom(ColumnFamilyStore.createColumnFamilyStore(keyspace,
                    newCFMetaData.cfName,
                    newCFMetaData,
                    new Directories(newCFMetaData),
                    false,
                    false,
                    true));

            final org.apache.cassandra.schema.KeyspaceMetadata current = Schema.instance.getKSMetaData(newCFMetaData.ksName);
            if (current == null) {
                LOGGER.warn("Keyspace {} doesn't exist", newCFMetaData.ksName);
                return;
            }
            if (current.tables.get(tableMetadata.getName().toString()).isPresent()) {
                LOGGER.debug("Table {}.{} is already added!", tableMetadata.getKeyspace(), tableMetadata.getName());
                return;
            }
            org.apache.cassandra.schema.KeyspaceMetadata transformed = current.withSwapped(Tables.of(newCFMetaData));
            Schema.instance.setKeyspaceMetadata(transformed);
            if (Schema.instance.hasCF(Pair.create(newCFMetaData.ksName, newCFMetaData.cfName))) {
                Schema.instance.unload(newCFMetaData);
            }
            Schema.instance.load(newCFMetaData);
            LOGGER.info("Added table [{}] to schema instance.", tableMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while adding table {}.{} to schema instance.", tableMetadata.getKeyspace(), tableMetadata.getName(), e);
        }
    }

    @Override
    public void onTableDropped(final TableMetadata tableMetadata) {
        Object cdc = tableMetadata.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean cdcEnabled = cdc.toString().equals("true");

        if (cdcEnabled) {
            schemaHolder.removeTableSchema(new KeyspaceTable(tableMetadata));
        }
        try {
            final String ksName = tableMetadata.getKeyspace().asInternal();
            final String tableName = tableMetadata.getName().asInternal();
            LOGGER.debug(String.format("Table %s.%s with id %s detected to be removed!", ksName, tableName, tableMetadata.getId().get()));
            final org.apache.cassandra.schema.KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(ksName);
            if (oldKsm == null) {
                LOGGER.warn("KeyspaceMetadata for keyspace {} is not found!", ksName);
                return;
            }

            final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableMetadata.getId().get());
            if (cfs == null) {
                LOGGER.warn("ColumnFamilyStore for {}.{} is not found!", ksName, tableName);
                return;
            }
            // make sure all the indexes are dropped, or else.
            cfs.indexManager.markAllIndexesRemoved();
            // reinitialize the keyspace.
            final Optional<CFMetaData> cfm = oldKsm.tables.get(tableName);

            if (cfm.isPresent()) {
                final org.apache.cassandra.schema.KeyspaceMetadata newKsm = oldKsm.withSwapped(oldKsm.tables.without(tableName));
                Schema.instance.unload(cfm.get());
                Schema.instance.setKeyspaceMetadata(newKsm);
                LOGGER.info("Removed table [{}] from schema instance.", tableMetadata.describe(true));
            }
            else {
                LOGGER.warn("Table {}.{} is not present in old keyspace meta data!", ksName, tableName);
            }

        }
        catch (Exception e) {
            LOGGER.warn("Error happened while removing table {}.{} from schema instance.",
                    tableMetadata.getKeyspace(),
                    tableMetadata.getName(),
                    e);
        }
    }

    @Override
    public void onTableUpdated(final TableMetadata newTableMetadata, final TableMetadata oldTableMetaData) {
        Object newCdcObject = newTableMetadata.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean newCdc = newCdcObject.toString().equals("true");
        Object oldCdcObject = oldTableMetaData.getOptions().get(CqlIdentifier.fromInternal("cdc"));
        boolean oldCdc = oldCdcObject.toString().equals("true");

        if (newCdc) {
            // if it was cdc before and now it is too, add it, because its schema might change
            // however if it is CDC-enabled but it was not, update it in schema too because its cdc flag has changed
            // this basically means we add / update every time if new has cdc flag equals to true
            schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(newTableMetadata),
                    new KeyValueSchema(kafkaTopicPrefix, newTableMetadata, sourceInfoStructMaker));
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
            final CFMetaData rawNewMetadata = CFMetaData.compile(newTableMetadata.describe(true),
                    newTableMetadata.getKeyspace().toString());
            final CFMetaData oldMetadata = Schema.instance.getCFMetaData(oldTableMetaData.getKeyspace().toString(), oldTableMetaData.getName().toString());
            // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
            final CFMetaData newMetadata = rawNewMetadata.copy(oldMetadata.cfId);
            oldMetadata.apply(newMetadata);
            LOGGER.info("Updated table [{}] in schema instance.", newTableMetadata.describe(true));
        }
        catch (Exception e) {
            LOGGER.warn("Error happened while reacting on changed table {}.{} in schema instance.", newTableMetadata.getKeyspace(), newTableMetadata.getName(), e);
        }
    }
}
