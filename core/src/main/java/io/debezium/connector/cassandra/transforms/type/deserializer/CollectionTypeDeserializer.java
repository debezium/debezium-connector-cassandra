/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.List;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class CollectionTypeDeserializer extends AbstractTypeDeserializer {

    public CollectionTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Class<?> abstractTypeClass) {
        super(deserializer, dataType, abstractTypeClass);
    }

    public abstract Object deserialize(Object abstractType, List<ByteBuffer> bbList);
}
