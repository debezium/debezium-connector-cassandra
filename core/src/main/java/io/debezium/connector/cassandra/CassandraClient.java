/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.codahale.metrics.MetricRegistry.name;
import static io.debezium.connector.cassandra.CassandraConnectorTaskTemplate.METRIC_REGISTRY_INSTANCE;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metrics.Metrics;

/**
 * A wrapper around Cassandra driver that is used to query Cassandra table and table schema.
 */
public class CassandraClient implements AutoCloseable {

    private final CqlSession session;

    public CassandraClient(String config, SchemaChangeListener schemaChangeListener) {
        session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(new File(config)))
                .withSchemaChangeListener(schemaChangeListener)
                .build();

        registerClusterMetrics(session.getMetadata().getClusterName().orElse("unknown-cluster-name"));
    }

    public Set<Node> getHosts() {
        return new HashSet<>(session.getMetadata().getNodes().values());
    }

    public String getClusterName() {
        return session.getMetadata().getClusterName().orElse("unknown-cluster-name");
    }

    public boolean isQueryable() {
        return !session.isClosed();
    }

    public ResultSet execute(SimpleStatement statement) {
        return session.execute(statement);
    }

    public ResultSet execute(String query) {
        return session.execute(query);
    }

    public ResultSet execute(String query, Object... values) {
        return session.execute(query, values);
    }

    public ResultSet execute(String query, Map<String, Object> values) {
        return session.execute(query, values);
    }

    public void shutdown() {
        if (!session.isClosed()) {
            session.close();
        }
    }

    private void registerClusterMetrics(String prefix) {
        Optional<Metrics> metrics = session.getMetrics();
        if (metrics.isPresent()) {
            MetricRegistry clusterRegistry = metrics.get().getRegistry();
            clusterRegistry.getMetrics().forEach((key, value) -> METRIC_REGISTRY_INSTANCE.register(name(prefix, key), value));
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
