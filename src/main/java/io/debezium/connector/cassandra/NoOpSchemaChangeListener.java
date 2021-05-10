/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.datastax.driver.core.AggregateMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.FunctionMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

public class NoOpSchemaChangeListener implements SchemaChangeListener {
    @Override
    public void onKeyspaceAdded(final KeyspaceMetadata keyspace) {

    }

    @Override
    public void onKeyspaceRemoved(final KeyspaceMetadata keyspace) {

    }

    @Override
    public void onKeyspaceChanged(final KeyspaceMetadata current, final KeyspaceMetadata previous) {

    }

    @Override
    public void onTableAdded(final TableMetadata table) {

    }

    @Override
    public void onTableRemoved(final TableMetadata table) {

    }

    @Override
    public void onTableChanged(final TableMetadata current, final TableMetadata previous) {

    }

    @Override
    public void onUserTypeAdded(final UserType type) {

    }

    @Override
    public void onUserTypeRemoved(final UserType type) {

    }

    @Override
    public void onUserTypeChanged(final UserType current, final UserType previous) {

    }

    @Override
    public void onFunctionAdded(final FunctionMetadata function) {

    }

    @Override
    public void onFunctionRemoved(final FunctionMetadata function) {

    }

    @Override
    public void onFunctionChanged(final FunctionMetadata current, final FunctionMetadata previous) {

    }

    @Override
    public void onAggregateAdded(final AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateRemoved(final AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateChanged(final AggregateMetadata current, final AggregateMetadata previous) {

    }

    @Override
    public void onMaterializedViewAdded(final MaterializedViewMetadata view) {

    }

    @Override
    public void onMaterializedViewRemoved(final MaterializedViewMetadata view) {

    }

    @Override
    public void onMaterializedViewChanged(final MaterializedViewMetadata current, final MaterializedViewMetadata previous) {

    }

    @Override
    public void onRegister(final Cluster cluster) {

    }

    @Override
    public void onUnregister(final Cluster cluster) {

    }
}
