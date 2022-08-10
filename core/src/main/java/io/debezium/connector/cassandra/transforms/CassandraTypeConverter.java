/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.type.converter.BasicTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.ListTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.MapTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.SetTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.TupleTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.TypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.UserTypeConverter;

public final class CassandraTypeConverter {

    private CassandraTypeConverter() {
    }

    private static final Map<Integer, TypeConverter<?>> typeMap = new HashMap<>();

    static {
        typeMap.put(ProtocolConstants.DataType.ASCII, new BasicTypeConverter<>(AsciiType.instance));
        typeMap.put(ProtocolConstants.DataType.BIGINT, new BasicTypeConverter<>(LongType.instance));
        typeMap.put(ProtocolConstants.DataType.BLOB, new BasicTypeConverter<>(BytesType.instance));
        typeMap.put(ProtocolConstants.DataType.BOOLEAN, new BasicTypeConverter<>(BooleanType.instance));
        typeMap.put(ProtocolConstants.DataType.COUNTER, new BasicTypeConverter<>(CounterColumnType.instance));
        typeMap.put(ProtocolConstants.DataType.DATE, new BasicTypeConverter<>(SimpleDateType.instance));
        typeMap.put(ProtocolConstants.DataType.DECIMAL, new BasicTypeConverter<>(DecimalType.instance));
        typeMap.put(ProtocolConstants.DataType.DOUBLE, new BasicTypeConverter<>(DoubleType.instance));
        typeMap.put(ProtocolConstants.DataType.DURATION, new BasicTypeConverter<>(DurationType.instance));
        typeMap.put(ProtocolConstants.DataType.FLOAT, new BasicTypeConverter<>(FloatType.instance));
        typeMap.put(ProtocolConstants.DataType.INET, new BasicTypeConverter<>(InetAddressType.instance));
        typeMap.put(ProtocolConstants.DataType.INT, new BasicTypeConverter<>(Int32Type.instance));
        typeMap.put(ProtocolConstants.DataType.LIST, new ListTypeConverter());
        typeMap.put(ProtocolConstants.DataType.MAP, new MapTypeConverter());
        typeMap.put(ProtocolConstants.DataType.SET, new SetTypeConverter());
        typeMap.put(ProtocolConstants.DataType.SMALLINT, new BasicTypeConverter<>(ShortType.instance));
        typeMap.put(ProtocolConstants.DataType.VARCHAR, new BasicTypeConverter<>(UTF8Type.instance));
        typeMap.put(ProtocolConstants.DataType.TIME, new BasicTypeConverter<>(TimeType.instance));
        typeMap.put(ProtocolConstants.DataType.TIMESTAMP, new BasicTypeConverter<>(TimestampType.instance));
        typeMap.put(ProtocolConstants.DataType.TIMEUUID, new BasicTypeConverter<>(TimeUUIDType.instance));
        typeMap.put(ProtocolConstants.DataType.TINYINT, new BasicTypeConverter<>(ByteType.instance));
        typeMap.put(ProtocolConstants.DataType.TUPLE, new TupleTypeConverter());
        typeMap.put(ProtocolConstants.DataType.UDT, new UserTypeConverter());
        typeMap.put(ProtocolConstants.DataType.UUID, new BasicTypeConverter<>(UUIDType.instance));
    }

    public static AbstractType<?> convert(com.datastax.oss.driver.api.core.type.DataType type) {
        TypeConverter<?> typeConverter = typeMap.get(type.getProtocolCode());
        return typeConverter.convert(type);
    }
}
