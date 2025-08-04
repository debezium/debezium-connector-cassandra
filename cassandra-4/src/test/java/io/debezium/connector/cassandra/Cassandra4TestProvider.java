/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;
import io.debezium.connector.cassandra.spi.CassandraTestProvider;
import io.debezium.connector.cassandra.spi.CommitLogProcessing;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class Cassandra4TestProvider implements CassandraTestProvider {
    @Override
    public CassandraConnectorContext provideContext(Configuration configuration) throws Exception {
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra4TypeProvider provider = new Cassandra4TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new DefaultCassandraConnectorContext(config,
                new CassandraConnectorTask.Cassandra4SchemaLoader(),
                new CassandraConnectorTask.Cassandra4SchemaChangeListenerProvider(),
                new FileOffsetWriter(config));
    }

    @Override
    public CassandraConnectorContext provideContextWithoutSchemaManagement(Configuration configuration) {
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra4TypeProvider provider = new Cassandra4TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new DefaultCassandraConnectorContext(new CassandraConnectorConfig(configuration));
    }

    @Override
    public CommitLogProcessing provideCommitLogProcessing(CassandraConnectorContext context, CassandraStreamingMetrics metrics) {
        return new Cassandra4CommitLogProcessing(context, metrics);
    }

    private static class Cassandra4CommitLogProcessing implements CommitLogProcessing {

        private final CommitLogReadHandler commitLogReadHandler;
        private final CommitLogSegmentReader commitLogSegmentReader;
        private final CassandraConnectorContext context;

        Cassandra4CommitLogProcessing(CassandraConnectorContext context, CassandraStreamingMetrics metrics) {
            commitLogReadHandler = new Cassandra4CommitLogReadHandlerImpl(context, metrics);
            commitLogSegmentReader = new Cassandra4CommitLogSegmentReader(context, metrics);
            this.context = context;
        }

        @Override
        public void readAllCommitLogs(File[] commitLogs) throws IOException {
            CommitLogReader reader = new CommitLogReader();
            File cdcLoc = Paths.get("target/data/cassandra/cdc_raw").toAbsolutePath().toFile();
            for (File commitLog : CommitLogUtil.getCommitLogs(cdcLoc)) {
                reader.readCommitLogSegment(commitLogReadHandler, new org.apache.cassandra.io.util.File(commitLog), true);
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
