/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.CassandraSchemaFactory.CellData;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;

/**
 * This reader is responsible for initial bootstrapping of a table,
 * which entails converting each row into a change event and enqueueing
 * that event to the {@link ChangeEventQueue}.
 * <p>
 * IMPORTANT: Currently, only when a snapshot is completed will the OffsetWriter
 * record the table in the offset.properties file (with filename "" and position
 * -1). This means if the SnapshotProcessor is terminated midway, upon restart
 * it will skip all the tables that are already recorded in offset.properties
 */
public class SnapshotProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotProcessor.class);

    private static final String NAME = "Snapshot Processor";
    private static final String CASSANDRA_NOW_UNIXTIMESTAMP = "TOUNIXTIMESTAMP(NOW())";
    private static final String EXECUTION_TIME_ALIAS = "execution_time";
    private static final Set<Integer> collectionTypes = Collect.unmodifiableSet(ProtocolConstants.DataType.LIST,
            ProtocolConstants.DataType.SET,
            ProtocolConstants.DataType.MAP);
    private static final CassandraSchemaFactory schemaFactory = CassandraSchemaFactory.get();

    private final CassandraClient cassandraClient;
    private final List<ChangeEventQueue<Event>> queues;
    private final OffsetWriter offsetWriter;
    private final SchemaHolder schemaHolder;
    private final RecordMaker recordMaker;
    private final CassandraConnectorConfig.SnapshotMode snapshotMode;
    private final ConsistencyLevel consistencyLevel;
    private final Set<String> startedTableNames = new HashSet<>();
    private final SnapshotProcessorMetrics metrics = new SnapshotProcessorMetrics();
    private boolean initial = true;
    private final String clusterName;

    public SnapshotProcessor(CassandraConnectorContext context, String clusterName) {
        super(NAME, context.getCassandraConnectorConfig().snapshotPollInterval());
        this.queues = context.getQueues();
        cassandraClient = context.getCassandraClient();
        offsetWriter = context.getOffsetWriter();
        schemaHolder = context.getSchemaHolder();
        recordMaker = new RecordMaker(context.getCassandraConnectorConfig().tombstonesOnDelete(),
                new Filters(context.getCassandraConnectorConfig().fieldExcludeList()),
                context.getCassandraConnectorConfig());
        snapshotMode = context.getCassandraConnectorConfig().snapshotMode();
        consistencyLevel = context.getCassandraConnectorConfig().snapshotConsistencyLevel();
        this.clusterName = clusterName;
    }

    @Override
    public void initialize() {
        metrics.registerMetrics();
    }

    @Override
    public void destroy() {
        metrics.unregisterMetrics();
    }

    @Override
    public void process() throws IOException {
        if (snapshotMode == CassandraConnectorConfig.SnapshotMode.ALWAYS) {
            snapshot();
        }
        else if (snapshotMode == CassandraConnectorConfig.SnapshotMode.INITIAL && initial) {
            snapshot();
            initial = false;
        }
        else {
            LOGGER.debug("Skipping snapshot [mode: {}]", snapshotMode);
        }
    }

    /**
     * Fetch for all new tables that have not yet been snapshotted, and then iterate through the
     * tables to snapshot each one of them.
     */
    public synchronized void snapshot() throws IOException {
        Set<TableMetadata> tables = getTablesToSnapshot();
        if (!tables.isEmpty()) {
            String[] tableArr = tables.stream().map(SnapshotProcessor::tableName).toArray(String[]::new);
            LOGGER.debug("Found {} tables to snapshot: {}", tables.size(), tableArr);
            long startTime = System.currentTimeMillis();
            metrics.setTableCount(tables.size());
            metrics.startSnapshot();
            for (TableMetadata table : tables) {
                if (isRunning()) {
                    String tableName = tableName(table);
                    LOGGER.info("Snapshotting table {} ...", tableName);
                    startedTableNames.add(tableName);
                    takeTableSnapshot(table);
                    metrics.completeTable();
                    LOGGER.info("Snapshot of table {} has been taken", tableName);
                }
            }
            metrics.stopSnapshot();
            long endTime = System.currentTimeMillis();
            long durationInSeconds = Duration.ofMillis(endTime - startTime).getSeconds();
            LOGGER.debug("Snapshot completely queued in {} seconds for tables: {}", durationInSeconds, tableArr);
        }
        else {
            LOGGER.info("No table to snapshot");
        }
    }

    /**
     * Return a set of {@link TableMetadata} for tables that have not been snapshotted but have CDC enabled.
     */
    private Set<TableMetadata> getTablesToSnapshot() {
        LOGGER.info("Present tables: {}", schemaHolder.getCdcEnabledTableMetadataSet().stream().map(tmd -> tmd.describe(true)).collect(Collectors.toList()));

        return schemaHolder.getCdcEnabledTableMetadataSet().stream()
                .filter(tm -> !offsetWriter.isOffsetProcessed(tableName(tm), OffsetPosition.defaultOffsetPosition().serialize(), true))
                .filter(tm -> !startedTableNames.contains(tableName(tm)))
                .collect(Collectors.toSet());
    }

    /**
     * Runs a SELECT query on a given table and process each row in the result set
     * by converting the row into a record and enqueue it to {@link ChangeRecord}
     */
    private void takeTableSnapshot(TableMetadata tableMetadata) throws IOException {
        try {
            SimpleStatement statement = generateSnapshotStatement(tableMetadata).setConsistencyLevel(DefaultConsistencyLevel.valueOf(consistencyLevel.name()));
            LOGGER.info("Executing snapshot query '{}' with consistency level {}", statement.getQuery(), statement.getConsistencyLevel());
            ResultSet resultSet = cassandraClient.execute(statement);
            LOGGER.info("Executed snapshot query for table {}", tableName(tableMetadata));
            processResultSet(tableMetadata, resultSet);
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Failed to snapshot table %s in keyspace %s", tableMetadata.getName(), tableMetadata.getKeyspace()), e);
        }
    }

    /**
     * Build the SELECT query statement for execution. For every non-primary-key column, the TTL, WRITETIME, and execution
     * time are also queried.
     * <p>
     * For example, a table t with columns a, b, and c, where A is the partition key, B is the clustering key, and C is a
     * regular column, looks like the following:
     * <pre>
     *     {@code SELECT now() as execution_time, a, b, c, TTL(c) as c_ttl, WRITETIME(c) as c_writetime FROM t;}
     * </pre>
     */
    private static SimpleStatement generateSnapshotStatement(TableMetadata tableMetadata) {
        List<String> allCols = tableMetadata.getColumns().values().stream().map(cmd -> cmd.getName().asInternal()).collect(Collectors.toList());
        Set<String> primaryCols = tableMetadata.getPrimaryKey().stream().map(cmd -> cmd.getName().asInternal()).collect(Collectors.toSet());
        List<String> collectionCols = tableMetadata.getColumns()
                .values()
                .stream()
                .filter(cm -> collectionTypes.contains(cm.getType().getProtocolCode()))
                .map(cmd -> cmd.getName().asInternal()).collect(Collectors.toList());

        SelectFrom selection = QueryBuilder.selectFrom(tableMetadata.getKeyspace(), tableMetadata.getName());
        assert !allCols.isEmpty();
        Select select = null;

        for (String col : allCols) {
            if (select == null) {
                select = selection.column(col);
            }
            else {
                select = select.column(col);
            }

            if (!primaryCols.contains(col) && !collectionCols.contains(col)) {
                select = select.ttl(withQuotes(col)).as(ttlAlias(col));
            }
        }

        return select.raw(CASSANDRA_NOW_UNIXTIMESTAMP).as(EXECUTION_TIME_ALIAS).build();
    }

    /**
     * Process the result set from the query. Each row is converted into a {@link ChangeRecord}
     * and enqueued to the {@link ChangeEventQueue}.
     */
    private void processResultSet(TableMetadata tableMetadata, ResultSet resultSet) throws IOException {
        String tableName = tableName(tableMetadata);
        KeyspaceTable keyspaceTable = new KeyspaceTable(tableMetadata);
        KeyValueSchema keyValueSchema = schemaHolder.getKeyValueSchema(keyspaceTable);
        Schema keySchema = keyValueSchema.keySchema();
        Schema valueSchema = keyValueSchema.valueSchema();

        Set<String> partitionKeyNames = tableMetadata.getPartitionKey().stream().map(cmd -> cmd.getName().toString()).collect(Collectors.toSet());
        Set<String> clusteringKeyNames = tableMetadata.getClusteringColumns().keySet().stream().map(cc -> cc.getName().toString()).collect(Collectors.toSet());

        Iterator<Row> rowIter = resultSet.iterator();
        long rowNum = 0L;
        // mark snapshot complete immediately if table is empty
        if (!rowIter.hasNext()) {
            offsetWriter.markOffset(tableName, OffsetPosition.defaultOffsetPosition().serialize(), true);
            offsetWriter.flush();
        }

        while (rowIter.hasNext()) {
            if (isRunning()) {
                Row row = rowIter.next();
                Object executionTime = readExecutionTime(row);
                RowData after = extractRowData(row, tableMetadata.getColumns().values(), partitionKeyNames, clusteringKeyNames, executionTime);
                // only mark offset if there are no more rows left
                boolean markOffset = !rowIter.hasNext();
                recordMaker.insert(clusterName, OffsetPosition.defaultOffsetPosition(),
                        keyspaceTable, true, Conversions.toInstantFromMicros(TimeUnit.MICROSECONDS.convert((long) executionTime, TimeUnit.MILLISECONDS)),
                        after, keySchema, valueSchema, markOffset, queues.get(Math.abs(tableName.hashCode() % queues.size()))::enqueue);
                rowNum++;
                if (rowNum % 10_000 == 0) {
                    LOGGER.debug("Queued {} snapshot records from table {}", rowNum, tableName);
                    metrics.setRowsScanned(tableName, rowNum);
                }
            }
            else {
                LOGGER.warn("Terminated snapshot processing while table {} is in progress", tableName);
                metrics.setRowsScanned(tableName, rowNum);
                return;
            }
        }
        metrics.setRowsScanned(tableName, rowNum);
    }

    /**
     * This function extracts the relevant row data from {@link Row} and updates the maximum writetime for each row.
     */
    private static RowData extractRowData(Row row,
                                          Collection<ColumnMetadata> columns,
                                          Set<String> partitionKeyNames,
                                          Set<String> clusteringKeyNames,
                                          Object executionTime) {
        RowData rowData = schemaFactory.rowData();

        for (ColumnMetadata columnMetadata : columns) {
            String name = columnMetadata.getName().asInternal();
            Object value = readCol(row, name, columnMetadata);
            Object deletionTs = null;
            CellData.ColumnType type = getType(name, partitionKeyNames, clusteringKeyNames);

            if (type == CellData.ColumnType.REGULAR
                    && value != null
                    && !collectionTypes.contains(columnMetadata.getType().getProtocolCode())) {
                Object ttl = readColTtl(row, name);
                if (ttl != null && executionTime != null) {
                    deletionTs = calculateDeletionTs(executionTime, ttl);
                }
            }

            CellData cellData = schemaFactory.cellData(name, value, deletionTs, type);
            rowData.addCell(cellData);
        }

        return rowData;
    }

    private static CellData.ColumnType getType(String name, Set<String> partitionKeyNames, Set<String> clusteringKeyNames) {
        if (partitionKeyNames.contains(name)) {
            return CellData.ColumnType.PARTITION;
        }
        else if (clusteringKeyNames.contains(name)) {
            return CellData.ColumnType.CLUSTERING;
        }
        else {
            return CellData.ColumnType.REGULAR;
        }
    }

    private static Object readExecutionTime(Row row) {
        return CassandraTypeDeserializer.deserialize(DataTypes.BIGINT, row.getBytesUnsafe(EXECUTION_TIME_ALIAS));
    }

    private static Object readCol(Row row, String col, ColumnMetadata cm) {
        return CassandraTypeDeserializer.deserialize(cm.getType(), row.getBytesUnsafe(col));
    }

    private static Object readColTtl(Row row, String col) {
        if (row.getColumnDefinitions().contains(CqlIdentifier.fromInternal(ttlAlias(col)))) {
            return CassandraTypeDeserializer.deserialize(DataTypes.COUNTER, row.getBytesUnsafe(ttlAlias(col)));
        }
        return null;
    }

    /**
     * it is not possible to query deletion time via cql, so instead calculate it from execution time (in milliseconds) + ttl (in seconds)
     */
    private static long calculateDeletionTs(Object executionTime, Object ttl) {
        return TimeUnit.MICROSECONDS.convert((long) executionTime, TimeUnit.MILLISECONDS) + TimeUnit.MICROSECONDS.convert((int) ttl, TimeUnit.SECONDS);
    }

    private static String ttlAlias(String colName) {
        return colName + "_ttl";
    }

    private static String withQuotes(String s) {
        return "\"" + s + "\"";
    }

    private static String tableName(TableMetadata tm) {
        return tm.getKeyspace() + "." + tm.getName();
    }
}
