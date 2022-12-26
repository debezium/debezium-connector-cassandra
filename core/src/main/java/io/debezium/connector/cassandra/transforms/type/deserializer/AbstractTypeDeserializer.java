/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class AbstractTypeDeserializer implements TypeDeserializer {

    private final DebeziumTypeDeserializer deserializer;
    private final Integer dataType;
    private final Class<?> abstractTypeClass;

    public AbstractTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Class<?> abstractTypeClass) {
        this.deserializer = deserializer;
        this.dataType = dataType;
        this.abstractTypeClass = abstractTypeClass;
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        return this.deserializer.deserialize(abstractType, bb);
    }

    public Integer getDataType() {
        return dataType;
    }

    public Class<?> getAbstractTypeClass() {
        return abstractTypeClass;
    }

}
