/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.COMPLETED_PREMATURELY;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.ERROR;

import java.util.List;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;

public class Cassandra4CommitLogBatchParser extends AbstractCassandra4CommitLogParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogBatchParser.class);

    public Cassandra4CommitLogBatchParser(LogicalCommitLog commitLog, List<ChangeEventQueue<Event>> queues,
                                          CommitLogProcessorMetrics metrics, CassandraConnectorContext context) {
        super(commitLog, queues, metrics, context);
    }

    @Override
    public CommitLogProcessingResult parse() {
        try {
            parseIndexFile(commitLog);

            while (!commitLog.completed) {
                if (completePrematurely) {
                    LOGGER.info("Processing of {} completed prematurely", commitLog);
                    return new CommitLogProcessingResult(commitLog, COMPLETED_PREMATURELY);
                }
                Thread.sleep(pollingInterval);
                parseIndexFile(commitLog);
            }

            LOGGER.info("Starting to read Commit log file: {} ", commitLog);
        }
        catch (final Exception ex) {
            LOGGER.error("Processing of {} errored out", commitLog, ex);
            return new CommitLogProcessingResult(commitLog, ERROR, ex);
        }

        CommitLogProcessingResult result;

        // process commit log from start to the end as it is completed at this point
        try {
            processCommitLog(commitLog, new CommitLogPosition(commitLog.commitLogSegmentId, 0));
            result = new CommitLogProcessingResult(commitLog);
        }
        catch (final Exception ex) {
            result = new CommitLogProcessingResult(commitLog, ERROR, ex);
        }

        LOGGER.info("Processing result: {}", result);
        return result;
    }
}
