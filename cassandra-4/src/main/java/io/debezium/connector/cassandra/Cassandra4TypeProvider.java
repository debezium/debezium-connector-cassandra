/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.BOOLEAN_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.BYTES_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.BYTE_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.DATE_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.DOUBLE_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.FLOAT_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.INT_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.LONG_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.SHORT_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.STRING_TYPE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.DurationTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.ListTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.MapTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.SetTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.TupleTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.UserDefinedTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.BasicTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.DecimalTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.InetAddressDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TimeUUIDTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TimestampTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.UUIDTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.VarIntTypeDeserializer;

public class Cassandra4TypeProvider implements CassandraTypeProvider {

    @Override
    public List<AbstractTypeDeserializer> deserializers() {
        DebeziumTypeDeserializer deserializer = new DebeziumTypeDeserializer() {
            @Override
            public Object deserialize(Object abstractType, ByteBuffer bb) {
                return ((AbstractType<?>) abstractType).getSerializer().deserialize(bb);
            }
        };
        List<AbstractTypeDeserializer> deserializers = Arrays.asList(
                // Basic Types
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.BOOLEAN, BooleanType.instance, BOOLEAN_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.VARCHAR, UTF8Type.instance, STRING_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.ASCII, AsciiType.instance, STRING_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.TINYINT, ByteType.instance, BYTE_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.BLOB, BytesType.instance, BYTES_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.FLOAT, FloatType.instance, FLOAT_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.DOUBLE, DoubleType.instance, DOUBLE_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.INT, Int32Type.instance, INT_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.SMALLINT, ShortType.instance, SHORT_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.BIGINT, LongType.instance, LONG_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.TIME, TimeType.instance, LONG_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.COUNTER, CounterColumnType.instance, LONG_TYPE),
                new BasicTypeDeserializer(deserializer, ProtocolConstants.DataType.DATE, SimpleDateType.instance, DATE_TYPE),
                // Logical Types
                new InetAddressDeserializer(deserializer, InetAddressType.instance),
                new TimestampTypeDeserializer(deserializer, TimestampType.instance),
                new UUIDTypeDeserializer(deserializer, UUIDType.instance),
                new TimeUUIDTypeDeserializer(deserializer, TimeUUIDType.instance),
                new DecimalTypeDeserializer(deserializer, DecimalType.instance),
                new VarIntTypeDeserializer(deserializer, IntegerType.instance),
                new DurationTypeDeserializer(deserializer),
                // Collection Types
                new ListTypeDeserializer(deserializer),
                new SetTypeDeserializer(deserializer),
                new MapTypeDeserializer(deserializer),
                // Struct Types
                new TupleTypeDeserializer(deserializer),
                new UserDefinedTypeDeserializer(deserializer));
        return Collections.unmodifiableList(deserializers);
    }

    @Override
    public Function<Object, Object> baseTypeForReversedType() {
        return abstractType -> ((AbstractType<?>) abstractType).isReversed() ? ((ReversedType<?>) abstractType).baseType : abstractType;
    }

    @Override
    public String getClusterName() {
        return DatabaseDescriptor.getClusterName();
    }

}
