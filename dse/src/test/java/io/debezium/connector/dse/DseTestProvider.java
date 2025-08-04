/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorConfig;
import io.debezium.connector.cassandra.CassandraConnectorContext;
import io.debezium.connector.cassandra.CommitLogSegmentReader;
import io.debezium.connector.cassandra.CommitLogUtil;
import io.debezium.connector.cassandra.DefaultCassandraConnectorContext;
import io.debezium.connector.cassandra.FileOffsetWriter;
import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;
import io.debezium.connector.cassandra.spi.CassandraTestProvider;
import io.debezium.connector.cassandra.spi.CommitLogProcessing;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class DseTestProvider implements CassandraTestProvider {
    @Override
    public CassandraConnectorContext provideContext(Configuration configuration) throws Exception {
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        DseTypeProvider provider = new DseTypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new DefaultCassandraConnectorContext(config,
                new DseSchemaLoader(),
                new DseSchemaChangeListenerProvider(),
                new FileOffsetWriter(config));
    }

    @Override
    public CassandraConnectorContext provideContextWithoutSchemaManagement(Configuration configuration) {
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        DseTypeProvider provider = new DseTypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new DefaultCassandraConnectorContext(new CassandraConnectorConfig(configuration));
    }

    @Override
    public CommitLogProcessing provideCommitLogProcessing(CassandraConnectorContext context, CassandraStreamingMetrics metrics) {
        return new DseCommitLogProcessing(context, metrics);
    }

    private static class DseCommitLogProcessing implements CommitLogProcessing {

        private final CommitLogReadHandler commitLogReadHandler;
        private final CommitLogSegmentReader commitLogSegmentReader;
        private final CassandraConnectorContext context;

        DseCommitLogProcessing(CassandraConnectorContext context, CassandraStreamingMetrics metrics) {
            commitLogReadHandler = new DseCommitLogReadHandlerImpl(context, metrics);
            commitLogSegmentReader = new DseCommitLogSegmentReader(context, metrics);
            this.context = context;
        }

        @Override
        public void readAllCommitLogs(File[] commitLogs) throws IOException {
            CommitLogReader reader = new CommitLogReader();
            File cdcLoc = Paths.get("target/data/cassandra/cdc_raw").toAbsolutePath().toFile();
            for (File commitLog : CommitLogUtil.getCommitLogs(cdcLoc)) {
                reader.readCommitLogSegment(commitLogReadHandler, commitLog, true);
            }
        }

        @Override
        public void readCommitLogSegment(File file, long segmentId, int position) throws IOException {
            commitLogSegmentReader.readCommitLogSegment(file, segmentId, position);
        }

        @Override
        public CommitLogSegmentReader getCommitLogSegmentReader() {
            return commitLogSegmentReader;
        }
    }
}
