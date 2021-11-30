/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public class BasicTypeDeserializer implements TypeDeserializer {

    private final SchemaBuilder schemaBuilder;
    private final DebeziumTypeDeserializer deserializer;

    public BasicTypeDeserializer(DebeziumTypeDeserializer deserializer, SchemaBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
        this.deserializer = deserializer;
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        return this.deserializer.deserialize(abstractType, bb);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        return schemaBuilder;
    }
}
