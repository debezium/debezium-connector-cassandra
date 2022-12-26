/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import com.datastax.oss.driver.api.core.type.DataType;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

/**
 * For deserializing logical-type columns in Cassandra, like UUID, TIMEUUID, Duration, etc.
 */
public abstract class LogicalTypeDeserializer extends AbstractTypeDeserializer {

    private final Object abstractType;

    public LogicalTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Object abstractType) {
        super(deserializer, dataType, abstractType.getClass());
        this.abstractType = abstractType;
    }

    @Override
    public Object getAbstractType(DataType dataType) {
        return abstractType;
    }

    /**
     * Format deserialized value from Cassandra to an object that fits its Kafka Schema.
     * @param abstractType the AbstractType of a column in cassandra
     * @param value the deserialized value of a column in cassandra
     * @return the formatted object from deserialized value
     */
    public abstract Object formatDeserializedValue(Object abstractType, Object value);

}
