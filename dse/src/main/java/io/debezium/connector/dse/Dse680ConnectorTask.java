/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;

import io.debezium.connector.cassandra.AbstractConnectorTask;
import io.debezium.connector.cassandra.AbstractProcessor;
import io.debezium.connector.cassandra.CassandraConnectorConfig;
import io.debezium.connector.cassandra.CassandraConnectorContext;
import io.debezium.connector.cassandra.CassandraConnectorTaskTemplate;
import io.debezium.connector.cassandra.CommitLogProcessor;
import io.debezium.connector.cassandra.CommitLogProcessorMetrics;
import io.debezium.connector.cassandra.ComponentFactory;

public class Dse680ConnectorTask extends AbstractConnectorTask {

    @Override
    protected CassandraConnectorTaskTemplate init(CassandraConnectorConfig config, ComponentFactory factory) {
        CommitLogProcessorMetrics metrics = new CommitLogProcessorMetrics();
        return new CassandraConnectorTaskTemplate(config,
                new DseTypeProvider(),
                new DseSchemaLoader(),
                new DseSchemaChangeListenerProvider(),
                context -> new AbstractProcessor[]{ getCommitLogProcessor(context, metrics, new DseCommitLogReadHandlerImpl(context, metrics)) },
                factory);
    }

    protected AbstractProcessor getCommitLogProcessor(CassandraConnectorContext context, CommitLogProcessorMetrics metrics, CommitLogReadHandler handler) {
        return new CommitLogProcessor(context, metrics, new DseCommitLogSegmentReader(context, metrics), DatabaseDescriptor.getCDCLogLocation(),
                DatabaseDescriptor.getCommitLogLocation());
    }
}
