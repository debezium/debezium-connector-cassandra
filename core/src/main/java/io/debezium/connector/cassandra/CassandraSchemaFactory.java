/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.schema.SchemaFactory;

public class CassandraSchemaFactory extends SchemaFactory {

    public CassandraSchemaFactory() {
        super();
    }

    private static final CassandraSchemaFactory cassandraSchemaFactoryObject = new CassandraSchemaFactory();

    public static CassandraSchemaFactory get() {
        return cassandraSchemaFactoryObject;
    }
}
