/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * For deserializing logical-type columns in Cassandra, like UUID, TIMEUUID, Duration, etc.
 */
public abstract class LogicalTypeDeserializer implements TypeDeserializer {

    /**
     * Format deserialized value from Cassandra to an object that fits it's kafka Schema.
     * @param abstractType the {@link AbstractType} of a column in cassandra
     * @param value the deserialized value of a column in cassandra
     * @return the formatted object from deserialized value
     */
    public abstract Object formatDeserializedValue(AbstractType<?> abstractType, Object value);

}
