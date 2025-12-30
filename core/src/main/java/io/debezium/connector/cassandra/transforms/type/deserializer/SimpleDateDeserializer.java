/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

/**
 * Deserializer for Cassandra SimpleDateType (DATE column type).
 * Cassandra stores dates shifted by Integer.MIN_VALUE for byte-order comparability.
 * This deserializer converts the shifted value back to days since Unix epoch (1970-01-01).
 */
public class SimpleDateDeserializer extends LogicalTypeDeserializer {

    public SimpleDateDeserializer(DebeziumTypeDeserializer deserializer, Object abstractType) {
        super(deserializer, ProtocolConstants.DataType.DATE, abstractType);
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        return formatDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        return CassandraTypeKafkaSchemaBuilders.DATE_TYPE;
    }

    @Override
    public Object formatDeserializedValue(Object abstractType, Object value) {
        // Cassandra SimpleDateSerializer.deserialize() returns the raw shifted integer value.
        // To get days since epoch, we add Integer.MIN_VALUE back.
        // Example: shifted value -2147463203 + Integer.MIN_VALUE = 20445 days since epoch
        Integer shiftedDays = (Integer) value;
        return shiftedDays + Integer.MIN_VALUE;
    }
}
