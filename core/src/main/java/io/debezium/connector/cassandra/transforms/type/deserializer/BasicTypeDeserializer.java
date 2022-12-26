/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.driver.api.core.type.DataType;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public class BasicTypeDeserializer extends AbstractTypeDeserializer {

    private final Object abstractType;
    private final SchemaBuilder schemaBuilder;

    public BasicTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Object abstractType,
                                 SchemaBuilder schemaBuilder) {
        super(deserializer, dataType, abstractType.getClass());
        this.abstractType = abstractType;
        this.schemaBuilder = schemaBuilder;
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        return schemaBuilder;
    }

    @Override
    public Object getAbstractType(DataType dataType) {
        return abstractType;
    }

}
