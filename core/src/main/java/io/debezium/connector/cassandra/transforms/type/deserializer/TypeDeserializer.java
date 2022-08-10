/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.SchemaBuilder;

public interface TypeDeserializer {

    Object deserialize(AbstractType<?> abstractType, ByteBuffer bb);

    SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType);
}
