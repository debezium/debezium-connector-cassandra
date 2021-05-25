/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.util.stream.Collectors.toMap;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.NoOpSchemaChangeListener;
import org.apache.cassandra.schema.KeyspaceParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;

import io.debezium.connector.SourceInfoStructMaker;

/**
 * The schema processor is reponsible for handling changes occuring in
 * Cassandra via registered schema change listener into driver.
 */
public class SchemaProcessor extends AbstractProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaProcessor.class);

    private static final String NAME = "Schema Processor";
    private final SchemaHolder schemaHolder;
    private final CassandraClient cassandraClient;
    private final String kafkaTopicPrefix;
    private final SourceInfoStructMaker sourceInfoStructMaker;
    private final SchemaChangeListener schemaChangeListener;

    public SchemaProcessor(CassandraConnectorContext context) {
        super(NAME, context.getCassandraConnectorConfig().schemaPollInterval());
        schemaHolder = context.getSchemaHolder();
        this.cassandraClient = context.getCassandraClient();
        this.kafkaTopicPrefix = schemaHolder.kafkaTopicPrefix;
        this.sourceInfoStructMaker = schemaHolder.sourceInfoStructMaker;

        schemaChangeListener = new NoOpSchemaChangeListener() {
            @Override
            public void onKeyspaceAdded(final KeyspaceMetadata keyspace) {
                try {
                    Schema.instance.setKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                            keyspace.getName(),
                            KeyspaceParams.create(keyspace.isDurableWrites(),
                                    keyspace.getReplication())));
                    Keyspace.openWithoutSSTables(keyspace.getName());
                    LOGGER.info("Added keyspace {}", keyspace.asCQLQuery());
                }
                catch (Throwable t) {
                    LOGGER.error("Error happened while adding the keyspace {}", keyspace.getName(), t);
                }
            }

            @Override
            public void onKeyspaceChanged(final KeyspaceMetadata current, final KeyspaceMetadata previous) {
                try {
                    Schema.instance.updateKeyspace(current.getName(), KeyspaceParams.create(current.isDurableWrites(), current.getReplication()));
                    LOGGER.info("Updated keyspace {}", current.asCQLQuery());
                }
                catch (Throwable t) {
                    LOGGER.error("Error happened while updating the keyspace {}", current.getName(), t);
                }
            }

            @Override
            public void onKeyspaceRemoved(final KeyspaceMetadata keyspace) {
                try {
                    schemaHolder.removeSchemasOfAllTablesInKeyspace(keyspace.getName());
                    Schema.instance.clearKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                            keyspace.getName(),
                            KeyspaceParams.create(keyspace.isDurableWrites(),
                                    keyspace.getReplication())));
                    LOGGER.info("Removed keyspace {}", keyspace.asCQLQuery());
                }
                catch (Throwable t) {
                    LOGGER.error("Error happened while removing the keyspace {}", keyspace.getName(), t);
                }
            }

            @Override
            public void onTableAdded(final TableMetadata tableMetadata) {
                try {
                    LOGGER.debug("Table {}.{} detected to be added!", tableMetadata.getKeyspace().getName(), tableMetadata.getName());
                    if (tableMetadata.getOptions().isCDC()) {
                        schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(tableMetadata),
                                new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker));
                    }

                    final CFMetaData rawCFMetaData = CFMetaData.compile(tableMetadata.asCQLQuery(), tableMetadata.getKeyspace().getName());
                    // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
                    final CFMetaData newCFMetaData = rawCFMetaData.copy(tableMetadata.getId());

                    Keyspace.open(newCFMetaData.ksName).initCf(newCFMetaData, false);

                    final org.apache.cassandra.schema.KeyspaceMetadata current = Schema.instance.getKSMetaData(newCFMetaData.ksName);
                    if (current == null) {
                        LOGGER.warn("Keyspace {} doesn't exist", newCFMetaData.ksName);
                        return;
                    }

                    if (current.tables.get(tableMetadata.getName()).isPresent()) {
                        LOGGER.debug("Table {}.{} is already added!", tableMetadata.getKeyspace(), tableMetadata.getName());
                        return;
                    }

                    final java.util.function.Function<org.apache.cassandra.schema.KeyspaceMetadata, org.apache.cassandra.schema.KeyspaceMetadata> transformationFunction = ks -> ks
                            .withSwapped(ks.tables.with(newCFMetaData));

                    org.apache.cassandra.schema.KeyspaceMetadata transformed = transformationFunction.apply(current);

                    Schema.instance.setKeyspaceMetadata(transformed);
                    Schema.instance.load(newCFMetaData);

                    LOGGER.info("Added schema for table {}", tableMetadata.asCQLQuery());
                }
                catch (Throwable t) {
                    LOGGER.error(String.format("Error happend while adding table %s.%s", tableMetadata.getKeyspace(), tableMetadata.getName()), t);
                }
            }

            @Override
            public void onTableRemoved(final TableMetadata table) {
                try {
                    LOGGER.info(String.format("Table %s.%s detected to be removed!", table.getKeyspace().getName(), table.getName()));
                    if (table.getOptions().isCDC()) {
                        schemaHolder.removeTableSchema(new KeyspaceTable(table));
                    }

                    final String ksName = table.getKeyspace().getName();
                    final String tableName = table.getName();

                    final org.apache.cassandra.schema.KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(ksName);

                    if (oldKsm == null) {
                        LOGGER.warn("KeyspaceMetadata for keyspace {} is not found!", ksName);
                        return;
                    }

                    ColumnFamilyStore cfs;

                    try {
                        cfs = Keyspace.openWithoutSSTables(ksName).getColumnFamilyStore(tableName);
                    }
                    catch (final Exception ex) {
                        LOGGER.warn("ColumnFamilyStore for {}.{} is not found!", ksName, table.getName());
                        return;
                    }

                    // make sure all the indexes are dropped, or else.
                    cfs.indexManager.markAllIndexesRemoved();

                    // reinitialize the keyspace.
                    final Optional<CFMetaData> cfm = oldKsm.tables.get(tableName);

                    unregisterMBean(cfs);

                    if (cfm.isPresent()) {
                        final org.apache.cassandra.schema.KeyspaceMetadata newKsm = oldKsm.withSwapped(oldKsm.tables.without(tableName));
                        Schema.instance.unload(cfm.get());
                        Schema.instance.setKeyspaceMetadata(newKsm);
                        LOGGER.info("Removed schema for table {}", table.asCQLQuery());
                    }
                    else {
                        LOGGER.warn("Table {}.{} is not present in old keyspace meta data!", ksName, tableName);
                    }
                }
                catch (Throwable t) {
                    LOGGER.error(String.format("Error happened while removing table %s.%s", table.getKeyspace().getName(), table.getName()), t);
                }
            }

            @Override
            public void onTableChanged(final TableMetadata newTableMetadata, final TableMetadata oldTableMetaData) {
                try {
                    LOGGER.debug("Detected alternation in schema of {}.{} (previous cdc = {}, current cdc = {})",
                            newTableMetadata.getKeyspace().getName(),
                            newTableMetadata.getName(),
                            oldTableMetaData.getOptions().isCDC(),
                            newTableMetadata.getOptions().isCDC());

                    if (newTableMetadata.getOptions().isCDC()) {
                        // if it was cdc before and now it is too, add it, because its schema might change
                        // however if it is CDC-enabled but it was not, update it in schema too because its cdc flag has changed
                        // this basically means we add / update every time if new has cdc flag equals to true
                        schemaHolder.addOrUpdateTableSchema(new KeyspaceTable(newTableMetadata),
                                new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, newTableMetadata, sourceInfoStructMaker));
                    }
                    else if (oldTableMetaData.getOptions().isCDC()) {
                        // if new table is not on cdc anymore, and we see the old one was, remove it
                        schemaHolder.removeTableSchema(new KeyspaceTable(newTableMetadata));
                    }

                    // else if it was not cdc before nor now, do nothing with schema holder
                    // but add it to Cassandra for subsequent deserialization path in every case

                    final CFMetaData rawNewMetadata = CFMetaData.compile(newTableMetadata.asCQLQuery(),
                            newTableMetadata.getKeyspace().getName());

                    final CFMetaData oldMetadata = Schema.instance.getCFMetaData(oldTableMetaData.getKeyspace().getName(), oldTableMetaData.getName());

                    // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
                    final CFMetaData newMetadata = rawNewMetadata.copy(oldMetadata.cfId);
                    oldMetadata.apply(newMetadata);

                    LOGGER.info("Updated schema for table {}", newTableMetadata.asCQLQuery());
                }
                catch (Throwable t) {
                    LOGGER.error(String.format("Error happened while reacting on changed table %s.%s", newTableMetadata.getKeyspace(), newTableMetadata.getName()), t);
                }
            }
        };
    }

    @Override
    public void initialize() {
        // populate schema holder when Debezium first starts
        // because it would not be notified about that otherwise,
        // listener is triggered on changes, not when driver connects for the first time
        // so holder map would be empty
        final Map<KeyspaceTable, SchemaHolder.KeyValueSchema> tables = schemaHolder.getCdcEnabledTableMetadataSet()
                .stream()
                .collect(toMap(KeyspaceTable::new,
                        tableMetadata -> new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker)));

        tables.forEach(schemaHolder::addOrUpdateTableSchema);

        LOGGER.info("Registering schema change listener ...");
        cassandraClient.getCluster().register(schemaChangeListener);
    }

    @Override
    public void process() {
        // intentionally empty as this processor is reactive in fact
        // it is not querying what tables Cassandra has every n-seconds
        // but it is notified by driver about these changes once they happen
        // so there is nothing to do "periodically" as other processors do.
    }

    @Override
    public void destroy() {
        LOGGER.info("Unregistering schema change listener ...");
        cassandraClient.getCluster().unregister(schemaChangeListener);
        LOGGER.info("Clearing cdc keyspace / table map ... ");
        schemaHolder.tableToKVSchemaMap.clear();
    }
}
