/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.CLUSTERING;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.PARTITION;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.REGULAR;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.CassandraSchemaFactory.CellData;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RangeData;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.time.Conversions;

/**
 * Handler that implements {@link CommitLogReadHandler} interface provided by Cassandra source code.
 * <p>
 * This handler implementation processes each {@link Mutation} and invokes one of the registered partition handler
 * for each {@link PartitionUpdate} in the {@link Mutation} (a mutation could have multiple partitions if it is a batch update),
 * which in turn makes one or more record via the {@link RecordMaker} and enqueue the record into the {@link ChangeEventQueue}.
 */
public class Cassandra4CommitLogReadHandlerImpl implements CommitLogReadHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogReadHandlerImpl.class);

    private static final boolean MARK_OFFSET = true;

    private final List<ChangeEventQueue<Event>> queues;
    private final RecordMaker recordMaker;
    private final OffsetWriter offsetWriter;
    private final SchemaHolder schemaHolder;
    private final CommitLogProcessorMetrics metrics;
    private final RangeTombstoneContext<org.apache.cassandra.schema.TableMetadata> rangeTombstoneContext = new RangeTombstoneContext<>();
    private final CassandraSchemaFactory schemaFactory;

    Cassandra4CommitLogReadHandlerImpl(CassandraConnectorContext context, CommitLogProcessorMetrics metrics) {
        this.queues = context.getQueues();
        this.recordMaker = new RecordMaker(context.getCassandraConnectorConfig().tombstonesOnDelete(),
                new Filters(context.getCassandraConnectorConfig().fieldExcludeList()),
                context.getCassandraConnectorConfig());
        this.offsetWriter = context.getOffsetWriter();
        this.schemaHolder = context.getSchemaHolder();
        this.metrics = metrics;
        this.schemaFactory = CassandraSchemaFactory.get();
    }

    /**
     * A PartitionType represents the type of PartitionUpdate.
     */
    enum PartitionType {
        /**
         * a partition-level deletion where partition key = primary key (no clustering key)
         */
        PARTITION_KEY_ROW_DELETION,

        /**
         * a partition-level deletion where partition key + clustering key = primary key
         */
        PARTITION_AND_CLUSTERING_KEY_ROW_DELETION,

        /**
         * a row-level modification
         */
        ROW_LEVEL_MODIFICATION,

        /**
         * an update on materialized view
         */
        MATERIALIZED_VIEW,

        /**
         * an update on secondary index
         */
        SECONDARY_INDEX,

        /**
         * an update on a table that contains counter data type
         */
        COUNTER;

        static final Set<PartitionType> supportedPartitionTypes = new HashSet<>(Arrays.asList(PARTITION_KEY_ROW_DELETION,
                PARTITION_AND_CLUSTERING_KEY_ROW_DELETION,
                ROW_LEVEL_MODIFICATION));

        public static PartitionType getPartitionType(PartitionUpdate pu) {
            if (pu.metadata().isCounter()) {
                return COUNTER;
            }
            else if (pu.metadata().isView()) {
                return MATERIALIZED_VIEW;
            }
            else if (pu.metadata().isIndex()) {
                return SECONDARY_INDEX;
            }
            else if (isPartitionDeletion(pu) && hasClusteringKeys(pu)) {
                return PARTITION_AND_CLUSTERING_KEY_ROW_DELETION;
            }
            else if (isPartitionDeletion(pu) && !hasClusteringKeys(pu)) {
                return PARTITION_KEY_ROW_DELETION;
            }
            else {
                return ROW_LEVEL_MODIFICATION;
            }
        }

        public static boolean isValid(PartitionType type) {
            return supportedPartitionTypes.contains(type);
        }

        public static boolean hasClusteringKeys(PartitionUpdate pu) {
            return !pu.metadata().clusteringColumns().isEmpty();
        }

        public static boolean isPartitionDeletion(PartitionUpdate pu) {
            return pu.partitionLevelDeletion().markedForDeleteAt() > LivenessInfo.NO_TIMESTAMP;
        }
    }

    /**
     * A RowType represents different types of {@link Row}-level modifications in a Cassandra table.
     */
    enum RowType {
        /**
         * Single-row insert
         */
        INSERT,

        /**
         * Single-row update
         */
        UPDATE,

        /**
         * Single-row delete
         */
        DELETE,

        /**
         * A row-level deletion that deletes a range of keys.
         * For example: DELETE * FROM table WHERE partition_key = 1 AND clustering_key > 0;
         */
        RANGE_TOMBSTONE,

        /**
         * Unknown row-level operation
         */
        UNKNOWN;

        static final Set<RowType> supportedRowTypes = new HashSet<>(Arrays.asList(INSERT, UPDATE, DELETE, RANGE_TOMBSTONE));

        public static RowType getRowType(Unfiltered unfiltered) {
            if (unfiltered.isRangeTombstoneMarker()) {
                return RANGE_TOMBSTONE;
            }
            else if (unfiltered.isRow()) {
                Row row = (Row) unfiltered;
                if (isDelete(row)) {
                    return DELETE;
                }
                else if (isInsert(row)) {
                    return INSERT;
                }
                else if (isUpdate(row)) {
                    return UPDATE;
                }
            }
            return UNKNOWN;
        }

        public static boolean isValid(RowType rowType) {
            return supportedRowTypes.contains(rowType);
        }

        public static boolean isDelete(Row row) {
            return row.deletion().time().markedForDeleteAt() > LivenessInfo.NO_TIMESTAMP;
        }

        public static boolean isInsert(Row row) {
            return row.primaryKeyLivenessInfo().timestamp() > LivenessInfo.NO_TIMESTAMP;
        }

        public static boolean isUpdate(Row row) {
            return row.primaryKeyLivenessInfo().timestamp() == LivenessInfo.NO_TIMESTAMP;
        }
    }

    @Override
    public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor descriptor) {
        if (!mutation.trackedByCDC()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not tracked by cdc {}.{{}}",
                        mutation.getKeyspaceName(),
                        mutation.getPartitionUpdates()
                                .stream()
                                .map(pa -> pa.metadata().name)
                                .collect(Collectors.toSet()));
            }
            return;
        }

        metrics.setCommitLogPosition(entryLocation);

        for (PartitionUpdate pu : mutation.getPartitionUpdates()) {
            OffsetPosition offsetPosition = new OffsetPosition(descriptor.fileName(), entryLocation);
            KeyspaceTable keyspaceTable = new KeyspaceTable(mutation.getKeyspaceName(), pu.metadata().name);

            if (offsetWriter.isOffsetProcessed(keyspaceTable.name(), offsetPosition.serialize(), false)) {
                LOGGER.info("Mutation at {} for table {} already processed, skipping...", offsetPosition, keyspaceTable);
                return;
            }

            try {
                process(pu, offsetPosition, keyspaceTable);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to process PartitionUpdate %s at %s for table %s.",
                        pu, offsetPosition, keyspaceTable.name()), e);
            }
        }

        metrics.onSuccess();
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) {
        LOGGER.error("Unrecoverable error when reading commit log", exception);
        metrics.onUnrecoverableError();
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) {
        if (exception.permissible) {
            LOGGER.error("Encountered a permissible exception during log replay", exception);
        }
        else {
            LOGGER.error("Encountered a non-permissible exception during log replay", exception);
        }
        return false;
    }

    /**
     * Method which processes a partition update if it's valid (either a single-row partition-level
     * deletion or a row-level modification) or throw an exception if it isn't. The valid partition
     * update is then converted into a {@link Record} and enqueued to the {@link ChangeEventQueue}.
     */
    private void process(PartitionUpdate pu, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {
        PartitionType partitionType = PartitionType.getPartitionType(pu);

        if (!PartitionType.isValid(partitionType)) {
            LOGGER.warn("Encountered an unsupported partition type {}, skipping...", partitionType);
            return;
        }

        switch (partitionType) {
            case PARTITION_KEY_ROW_DELETION:
            case PARTITION_AND_CLUSTERING_KEY_ROW_DELETION:
                handlePartitionDeletion(pu, offsetPosition, keyspaceTable);
                break;
            case ROW_LEVEL_MODIFICATION:
                UnfilteredRowIterator it = pu.unfilteredIterator();
                while (it.hasNext()) {
                    Unfiltered rowOrRangeTombstone = it.next();
                    RowType rowType = RowType.getRowType(rowOrRangeTombstone);
                    if (!RowType.isValid(rowType)) {
                        LOGGER.warn("Encountered an unsupported row type {}, skipping...", rowType);
                        continue;
                    }
                    if (rowOrRangeTombstone instanceof Row) {
                        Row row = (Row) rowOrRangeTombstone;
                        handleRowModifications(row, rowType, pu, offsetPosition, keyspaceTable);
                    }
                    else if (rowOrRangeTombstone instanceof RangeTombstoneBoundMarker) {
                        handleRangeTombstoneBoundMarker((RangeTombstoneBoundMarker) rowOrRangeTombstone,
                                rowType, pu, offsetPosition, keyspaceTable);
                    }
                    else {
                        throw new CassandraConnectorSchemaException("Encountered unsupported Unfiltered type " + rowOrRangeTombstone.getClass());
                    }
                }
                break;

            default:
                throw new CassandraConnectorSchemaException("Unsupported partition type " + partitionType + " should have been skipped");
        }
    }

    /**
     * Handle a valid deletion event resulted from a partition-level deletion by converting Cassandra representation
     * of this event into a {@link Record} object and queue the record to {@link ChangeEventQueue}. A valid deletion
     * event means a partition only has a single row, this implies there are no clustering keys.
     *
     * The steps are:
     *      (1) Populate the "source" field for this event
     *      (2) Fetch the cached key/value schemas from {@link SchemaHolder}
     *      (3) Populate the "after" field for this event
     *          a. populate partition columns
     *          b. populate clustering columns if any
     *          b. populate regular columns with null values
     *      (4) Assemble a {@link Record} object from the populated data and queue the record
     */
    private void handlePartitionDeletion(PartitionUpdate pu, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {
        KeyValueSchema keyValueSchema = schemaHolder.getKeyValueSchema(keyspaceTable);
        if (keyValueSchema == null) {
            LOGGER.warn("Unable to get KeyValueSchema for table {}. It might have been deleted or CDC disabled.", keyspaceTable.toString());
            return;
        }

        Schema keySchema = keyValueSchema.keySchema();
        Schema valueSchema = keyValueSchema.valueSchema();
        TableMetadata tableMetadata = keyValueSchema.tableMetadata();

        RowData after = schemaFactory.rowData();

        populatePartitionColumns(after, pu);

        // For partition deletions, the PartitionUpdate only specifies the partition key, it does not
        // contain any info on regular (non-partition) columns, as if they were not modified. In order
        // to differentiate deleted columns from unmodified columns, we populate the deleted columns
        // with null value and timestamps

        // clustering columns if any

        List<ColumnMetadata> columns = new ArrayList<>(tableMetadata.getColumns().values());
        Map<ColumnMetadata, ClusteringOrder> clusteringColumns = tableMetadata.getClusteringColumns();

        for (Map.Entry<ColumnMetadata, ClusteringOrder> clustering : clusteringColumns.entrySet()) {
            ColumnMetadata clusteringKey = clustering.getKey();
            long deletionTs = pu.deletionInfo().getPartitionDeletion().markedForDeleteAt();
            after.addCell(schemaFactory.cellData(clusteringKey.getName().toString(), null, deletionTs, CLUSTERING));
        }

        columns.removeAll(tableMetadata.getPartitionKey());
        columns.removeAll(tableMetadata.getClusteringColumns().keySet());

        // regular columns if any

        for (ColumnMetadata cm : columns) {
            String name = cm.getName().toString();
            long deletionTs = pu.deletionInfo().getPartitionDeletion().markedForDeleteAt();
            CellData cellData = schemaFactory.cellData(name, null, deletionTs, REGULAR);
            after.addCell(cellData);
        }

        recordMaker.delete(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                Conversions.toInstantFromMicros(pu.maxTimestamp()), after, keySchema, valueSchema,
                MARK_OFFSET, queues.get(Math.abs(offsetPosition.fileName.hashCode() % queues.size()))::enqueue);
    }

    /**
     * Handle a valid event resulted from a row-level modification by converting Cassandra representation of
     * this event into a {@link Record} object and queue the record to {@link ChangeEventQueue}. A valid event
     * implies this must be an insert, update, or delete.
     *
     * The steps are:
     *      (1) Populate the "source" field for this event
     *      (2) Fetch the cached key/value schemas from {@link SchemaHolder}
     *      (3) Populate the "after" field for this event
     *          a. populate partition columns
     *          b. populate clustering columns
     *          c. populate regular columns
     *          d. for deletions, populate regular columns with null values
     *      (4) Assemble a {@link Record} object from the populated data and queue the record
     */
    private void handleRowModifications(Row row, RowType rowType, PartitionUpdate pu, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {
        KeyValueSchema keyValueSchema = schemaHolder.getKeyValueSchema(keyspaceTable);
        if (keyValueSchema == null) {
            LOGGER.trace("Unable to get KeyValueSchema for table {}. It might have been deleted or CDC disabled.", keyspaceTable.toString());
            return;
        }
        Schema keySchema = keyValueSchema.keySchema();
        Schema valueSchema = keyValueSchema.valueSchema();

        RowData after = schemaFactory.rowData();
        populatePartitionColumns(after, pu);
        populateClusteringColumns(after, row, pu);
        populateRegularColumns(after, row, rowType, keyValueSchema);

        long ts = rowType == RowType.DELETE ? row.deletion().time().markedForDeleteAt() : pu.maxTimestamp();

        switch (rowType) {
            case INSERT:
                recordMaker.insert(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET,
                        queues.get(Math.abs(offsetPosition.fileName.hashCode() % queues.size()))::enqueue);
                break;

            case UPDATE:
                recordMaker.update(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET,
                        queues.get(Math.abs(offsetPosition.fileName.hashCode() % queues.size()))::enqueue);
                break;

            case DELETE:
                recordMaker.delete(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET,
                        queues.get(Math.abs(offsetPosition.fileName.hashCode() % queues.size()))::enqueue);
                break;

            case RANGE_TOMBSTONE:
                recordMaker.rangeTombstone(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET,
                        queues.get(Math.abs(offsetPosition.fileName.hashCode() % queues.size()))::enqueue);
                break;

            default:
                throw new CassandraConnectorSchemaException("Unsupported row type " + rowType + " should have been skipped");
        }
    }

    private void handleRangeTombstoneBoundMarker(RangeTombstoneBoundMarker rangeTombstoneMarker,
                                                 RowType rowType,
                                                 PartitionUpdate pu,
                                                 OffsetPosition offsetPosition,
                                                 KeyspaceTable keyspaceTable) {
        if (rowType != RowType.RANGE_TOMBSTONE) {
            throw new IllegalStateException("Row type has to be " + RowType.RANGE_TOMBSTONE.name());
        }
        KeyValueSchema keyValueSchema = schemaHolder.getKeyValueSchema(keyspaceTable);
        if (keyValueSchema == null) {
            LOGGER.warn("Unable to get KeyValueSchema for table {}. It might have been deleted or CDC disabled.", keyspaceTable.toString());
            return;
        }

        RowData after = rangeTombstoneContext.getOrCreate(pu.metadata());

        Optional.ofNullable(rangeTombstoneMarker.openBound(false)).ifPresent(cb -> {
            after.addStartRange(populateRangeData(cb, RangeData.RANGE_START_NAME, pu.metadata()));
        });

        Optional.ofNullable(rangeTombstoneMarker.closeBound(false)).ifPresent(cb -> {
            after.addEndRange(populateRangeData(cb, RangeData.RANGE_END_NAME, pu.metadata()));
        });

        if (RangeTombstoneContext.isComplete(after)) {
            try {
                populatePartitionColumns(after, pu);
                long ts = rangeTombstoneMarker.deletionTime().markedForDeleteAt();

                recordMaker.rangeTombstone(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keyValueSchema.keySchema(), keyValueSchema.valueSchema(), MARK_OFFSET,
                        queues.get(Math.abs(offsetPosition.fileName.hashCode() % queues.size()))::enqueue);
            }
            finally {
                rangeTombstoneContext.remove(pu.metadata());
            }
        }
    }

    private RangeData populateRangeData(ClusteringBound<?> cb, String name, org.apache.cassandra.schema.TableMetadata metaData) {
        Map<String, Pair<String, String>> values = new HashMap<>();

        for (int i = 0; i < cb.size(); i++) {
            String clusteringColumnName = metaData.clusteringColumns().get(i).name.toCQLString();
            String clusteringColumnValue = metaData.comparator.subtype(i).getString(cb.bufferAt(i));
            String clusteringColumnType = metaData.clusteringColumns().get(i).type.toString();
            values.put(clusteringColumnName, Pair.of(clusteringColumnValue, clusteringColumnType));
        }

        return schemaFactory.rangeData(name, cb.kind().toString(), values);
    }

    private void populatePartitionColumns(RowData after, PartitionUpdate pu) {
        // if it has any cells it was already populated
        if (after.hasAnyCell()) {
            return;
        }
        List<Object> partitionKeys = getPartitionKeys(pu);
        for (org.apache.cassandra.schema.ColumnMetadata cd : pu.metadata().partitionKeyColumns()) {
            try {
                String name = cd.name.toString();
                Object value = partitionKeys.get(cd.position());
                CellData cellData = schemaFactory.cellData(name, value, null, PARTITION);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, pu.metadata().keyspace), e);
            }
        }
    }

    private void populateClusteringColumns(RowData after, Row row, PartitionUpdate pu) {
        for (org.apache.cassandra.schema.ColumnMetadata cd : pu.metadata().clusteringColumns()) {
            try {
                ByteBuffer bufferAtClustering = row.clustering().bufferAt(cd.position());
                Object value = CassandraTypeDeserializer.deserialize(cd.type, bufferAtClustering);
                CellData cellData = schemaFactory.cellData(cd.name.toString(), value, null, CLUSTERING);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, pu.metadata().keyspace), e);
            }
        }
    }

    private void populateRegularColumns(RowData after, Row row, RowType rowType, KeyValueSchema schema) {
        if (rowType == RowType.INSERT || rowType == RowType.UPDATE) {
            for (org.apache.cassandra.schema.ColumnMetadata cd : row.columns()) {
                try {
                    Object value;
                    Object deletionTs = null;
                    AbstractType<?> abstractType = cd.type;
                    if (abstractType.isCollection() && abstractType.isMultiCell()) {
                        ComplexColumnData ccd = row.getComplexColumnData(cd);
                        value = CassandraTypeDeserializer.deserialize((CollectionType<?>) abstractType, getComplexColumnDataByteBufferList(abstractType, ccd));
                    }
                    else {
                        org.apache.cassandra.db.rows.Cell<?> cell = row.getCell(cd);
                        value = cell.isTombstone() ? null : CassandraTypeDeserializer.deserialize(abstractType, cell.buffer());
                        deletionTs = cell.isExpiring() ? TimeUnit.MICROSECONDS.convert(cell.localDeletionTime(), TimeUnit.SECONDS) : null;
                    }
                    String name = cd.name.toString();
                    CellData cellData = schemaFactory.cellData(name, value, deletionTs, REGULAR);
                    after.addCell(cellData);
                }
                catch (Exception e) {
                    throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                            cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
                }
            }

        }
        else if (rowType == RowType.DELETE) {
            // For row-level deletions, row.columns() will result in an empty list and does not contain
            // the column definitions for the deleted columns. In order to differentiate deleted columns from
            // unmodified columns, we populate the deleted columns with null value and timestamps.
            TableMetadata tableMetadata = schema.tableMetadata();
            List<ColumnMetadata> columns = new ArrayList<>(tableMetadata.getColumns().values());
            columns.removeAll(tableMetadata.getPrimaryKey());
            for (ColumnMetadata cm : columns) {
                String name = cm.getName().toString();
                long deletionTs = row.deletion().time().markedForDeleteAt();
                CellData cellData = schemaFactory.cellData(name, null, deletionTs, REGULAR);
                after.addCell(cellData);
            }
        }
    }

    private List<ByteBuffer> getComplexColumnDataByteBufferList(AbstractType<?> abstractType, ComplexColumnData ccd) {
        if (abstractType instanceof ListType) {
            return ((ListType<?>) abstractType).serializedValues(ccd.iterator());
        }
        if (abstractType instanceof SetType) {
            return ((SetType<?>) abstractType).serializedValues(ccd.iterator());
        }
        if (abstractType instanceof MapType) {
            return ((MapType<?, ?>) abstractType).serializedValues(ccd.iterator());
        }
        throw new DebeziumException(String.format("Unknow collection type %s", abstractType));
    }

    /**
     * Given a PartitionUpdate, deserialize the partition key byte buffer
     * into a list of partition key values.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private List<Object> getPartitionKeys(PartitionUpdate pu) {
        List<Object> values = new ArrayList<>();

        List<org.apache.cassandra.schema.ColumnMetadata> columnDefinitions = pu.metadata().partitionKeyColumns();

        // simple partition key
        if (columnDefinitions.size() == 1) {
            ByteBuffer bb = pu.partitionKey().getKey();
            ColumnSpecification cs = columnDefinitions.get(0);
            AbstractType<?> type = cs.type;
            try {
                Object value = CassandraTypeDeserializer.deserialize(type, bb);
                values.add(value);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s.",
                        cs.name.toString(), type.toString(), pu.metadata().name, pu.metadata().keyspace), e);
            }

            // composite partition key
        }
        else {
            ByteBuffer keyBytes = pu.partitionKey().getKey().duplicate();

            // 0xFFFF is reserved to encode "static column", skip if it exists at the start
            if (keyBytes.remaining() >= 2) {
                int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                if ((header & 0xFFFF) == 0xFFFF) {
                    ByteBufferUtil.readShortLength(keyBytes);
                }
            }

            // the encoding of columns in the partition key byte buffer is
            // <col><col><col>...
            // where <col> is:
            // <length of value><value><end-of-component byte>
            // <length of value> is a 2 bytes unsigned short (excluding 0xFFFF used to encode "static columns")
            // <end-of-component byte> should always be 0 for columns (1 for query bounds)
            // this section reads the bytes for each column and deserialize into objects based on each column type
            int i = 0;
            while (keyBytes.remaining() > 0 && i < columnDefinitions.size()) {
                ColumnSpecification cs = columnDefinitions.get(i);
                AbstractType<?> type = cs.type;
                ByteBuffer bb = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                try {
                    Object value = CassandraTypeDeserializer.deserialize(type, bb);
                    values.add(value);
                }
                catch (Exception e) {
                    throw new DebeziumException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s",
                            cs.name.toString(), cs.type.toString(), cs.cfName, cs.ksName), e);
                }
                byte b = keyBytes.get();
                if (b != 0) {
                    break;
                }
                ++i;
            }
        }

        return values;
    }

}
