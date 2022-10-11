/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class CassandraPartition implements Partition {

    private final String nodeId;

    private final String logicalName;

    private final Map<String, String> sourcePartition;

    private final int hashCode;

    private CassandraPartition(String logicalName, String nodeId) {
        this.logicalName = logicalName;
        this.nodeId = nodeId;
        this.sourcePartition = Collect.hashMapOf("logicalName", logicalName, "nodeId", nodeId);
        this.hashCode = Objects.hash(logicalName, nodeId);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CassandraPartition other = (CassandraPartition) obj;
        return Objects.equals(logicalName, other.logicalName) && Objects.equals(nodeId, other.nodeId);
    }

    public static class Provider implements Partition.Provider<CassandraPartition> {

        private final CassandraConnectorConfig connectorConfig;

        public Provider(CassandraConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<CassandraPartition> getPartitions() {
            return Collections
                    .singleton(new CassandraPartition(connectorConfig.getLogicalName(), connectorConfig.getNodeId()));
        }
    }
}
