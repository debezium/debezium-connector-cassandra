/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

/**
 * If a concrete connector implementation uses some processor which is specific to that Cassandra version only,
 * implement this interface and use it in {@link CassandraConnectorTaskTemplate}.
 */
public interface CassandraSpecificProcessors {
    /**
     * Get Cassandra specific processors.
     *
     * @param context Cassandra context to use upon processors initialisation
     * @return processors specific to a respective Cassandra version only.
     */
    AbstractProcessor[] getProcessors(CassandraConnectorContext context);
}
