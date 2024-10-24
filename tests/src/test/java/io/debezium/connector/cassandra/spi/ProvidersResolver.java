/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.spi;

import java.util.Optional;
import java.util.ServiceLoader;

public class ProvidersResolver {

    public static CassandraTestProvider resolveConnectorContextProvider() {
        ServiceLoader<CassandraTestProvider> serviceLoader = ServiceLoader.load(CassandraTestProvider.class);

        Optional<CassandraTestProvider> first = serviceLoader.findFirst();

        if (first.isEmpty()) {
            throw new IllegalStateException("There is no provider of " + CassandraTestProvider.class.getName());
        }

        return first.get();
    }
}
