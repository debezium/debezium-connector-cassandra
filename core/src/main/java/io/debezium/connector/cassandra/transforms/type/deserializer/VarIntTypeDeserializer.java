/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.LONG_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.STRING_TYPE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.cassandra.CassandraConnectorConfig.VarIntHandlingMode;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public class VarIntTypeDeserializer extends LogicalTypeDeserializer {

    private final DebeziumTypeDeserializer deserializer;
    private VarIntHandlingMode mode;

    public VarIntTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        this.deserializer = deserializer;
        this.mode = VarIntHandlingMode.LONG;
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Object value = deserializer.deserialize(abstractType, bb);
        return formatDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        switch (mode) {
            case LONG:
                return LONG_TYPE;
            case PRECISE:
                return Decimal.builder(0);
            case STRING:
                return STRING_TYPE;
        }
        throw new IllegalArgumentException("Unknown varIntHandlingMode");
    }

    @Override
    public Object formatDeserializedValue(AbstractType<?> abstractType, Object value) {
        BigInteger bigint = (BigInteger) value;
        switch (mode) {
            case LONG:
                return bigint.longValue();
            case PRECISE:
                return new BigDecimal(bigint);
            case STRING:
                return bigint.toString();
        }
        throw new IllegalArgumentException("Unknown varIntHandlingMode");
    }

    public void setMode(VarIntHandlingMode mode) {
        this.mode = mode;
    }
}
