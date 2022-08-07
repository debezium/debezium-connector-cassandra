/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;

public class Cassandra4CommitLogBatchParser extends Cassandra4CommitLogParserBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogBatchParser.class);

    public Cassandra4CommitLogBatchParser(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog, List<ChangeEventQueue<Event>> queues,
                                          CommitLogProcessorMetrics metrics, CassandraConnectorContext context) {
        super(commitLog, queues, metrics, context);
    }

    @Override
    public Cassandra4CommitLogProcessor.ProcessingResult parse() {
        if (!commitLog.exists()) {
            LOGGER.warn("Commit log " + commitLog + " does not exist!");
            return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, Cassandra4CommitLogProcessor.ProcessingResult.Result.DOES_NOT_EXIST);
        }

        LOGGER.info("Processing commit log {}", commitLog.log.toString());

        CommitLogPosition position = new CommitLogPosition(commitLog.commitLogSegmentId, 0);
        metrics.setCommitLogFilename(commitLog.log.toString());
        metrics.setCommitLogPosition(position.position);

        try {
            parseIndexFile(commitLog);

            while (!commitLog.completed) {
                if (completePrematurely) {
                    LOGGER.info("{} completed prematurely", commitLog.toString());
                    return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, Cassandra4CommitLogProcessor.ProcessingResult.Result.COMPLETED_PREMATURELY);
                }
                // TODO make this configurable maybe
                Thread.sleep(10000);
                parseIndexFile(commitLog);
            }

            LOGGER.info("Starting to read Commit log file: {} ", commitLog.toString());
        }
        catch (final Exception ex) {
            LOGGER.error("Processing of {} errored out", commitLog.toString(), ex);
            return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, Cassandra4CommitLogProcessor.ProcessingResult.Result.ERROR, ex);
        }

        Cassandra4CommitLogProcessor.ProcessingResult result;

        // process commit log from start to the end as it is completed at this point
        try {
            processCommitLog(commitLog, new CommitLogPosition(commitLog.commitLogSegmentId, 0));
            result = new Cassandra4CommitLogProcessor.ProcessingResult(commitLog);
        }
        catch (final Exception ex) {
            result = new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, Cassandra4CommitLogProcessor.ProcessingResult.Result.ERROR, ex);
        }

        LOGGER.info("Processing result: {}", result);
        return result;
    }
}