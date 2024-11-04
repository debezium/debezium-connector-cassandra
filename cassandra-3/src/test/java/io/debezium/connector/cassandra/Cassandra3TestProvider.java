/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.spi.CassandraTestProvider;
import io.debezium.connector.cassandra.spi.CommitLogProcessing;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class Cassandra3TestProvider implements CassandraTestProvider {
    @Override
    public CommitLogProcessing provideCommitLogProcessing(CassandraConnectorContext context, CommitLogProcessorMetrics metrics) {
        return new Cassandra3CommitLogProcessing(context, metrics);
    }

    @Override
    public String getClusterName() {
        return DatabaseDescriptor.getClusterName();
    }

    private static class Cassandra3CommitLogProcessing implements CommitLogProcessing {

        private final CommitLogReadHandler commitLogReadHandler;
        private final CommitLogSegmentReader commitLogSegmentReader;

        Cassandra3CommitLogProcessing(CassandraConnectorContext context, CommitLogProcessorMetrics metrics) {
            commitLogReadHandler = new Cassandra3CommitLogReadHandlerImpl(context, metrics);
            commitLogSegmentReader = new Cassandra3CommitLogSegmentReader(context, metrics);
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

    @Override
    public CassandraConnectorContext provideContext(Configuration configuration) throws Exception {
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra3TypeProvider provider = new Cassandra3TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new DefaultCassandraConnectorContext(config,
                new CassandraConnectorTask.Cassandra3SchemaLoader(),
                new CassandraConnectorTask.Cassandra3SchemaChangeListenerProvider(),
                new FileOffsetWriter(config));
    }

    @Override
    public CassandraConnectorContext provideContextWithoutSchemaManagement(Configuration configuration) {
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra3TypeProvider provider = new Cassandra3TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new DefaultCassandraConnectorContext(new CassandraConnectorConfig(configuration));
    }
}
