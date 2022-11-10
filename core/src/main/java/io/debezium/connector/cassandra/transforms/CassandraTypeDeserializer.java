/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.driver.api.core.type.DataType;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.cassandra.CassandraConnectorConfig.VarIntHandlingMode;
import io.debezium.connector.cassandra.transforms.type.deserializer.BasicTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.CollectionTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.DurationTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.InetAddressDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.ListTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.MapTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.SetTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TimeUUIDTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TimestampTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TupleTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.UUIDTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.UserDefinedTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.VarIntTypeDeserializer;

@ThreadSafe
@Immutable
public final class CassandraTypeDeserializer {

    private Map<Class<? extends AbstractType>, TypeDeserializer> TYPE_MAP;

    private static CassandraTypeDeserializer instance;

    private CassandraTypeDeserializer() {
    }

    public static CassandraTypeDeserializer getInstance() {
        if (CassandraTypeDeserializer.instance == null) {
            synchronized (CassandraTypeDeserializer.class) {
                if (CassandraTypeDeserializer.instance == null) {
                    CassandraTypeDeserializer.instance = new CassandraTypeDeserializer();
                }
            }
        }
        return CassandraTypeDeserializer.instance;
    }

    public static void init(DebeziumTypeDeserializer typeDeserializer) {
        CassandraTypeDeserializer instance = getInstance();
        instance.initInternal(typeDeserializer);
    }

    private void initInternal(DebeziumTypeDeserializer deserializer) {
        if (TYPE_MAP != null) {
            return;
        }

        Map<Class<? extends AbstractType>, TypeDeserializer> tmp = new HashMap<>();

        // Basic Types
        tmp.put(BooleanType.class, new BasicTypeDeserializer(deserializer, BOOLEAN_TYPE));
        tmp.put(UTF8Type.class, new BasicTypeDeserializer(deserializer, STRING_TYPE));
        tmp.put(AsciiType.class, new BasicTypeDeserializer(deserializer, STRING_TYPE));
        tmp.put(ByteType.class, new BasicTypeDeserializer(deserializer, BYTE_TYPE));
        tmp.put(BytesType.class, new BasicTypeDeserializer(deserializer, BYTES_TYPE));
        tmp.put(FloatType.class, new BasicTypeDeserializer(deserializer, FLOAT_TYPE));
        tmp.put(DoubleType.class, new BasicTypeDeserializer(deserializer, DOUBLE_TYPE));
        tmp.put(DecimalType.class, new BasicTypeDeserializer(deserializer, DOUBLE_TYPE));
        tmp.put(Int32Type.class, new BasicTypeDeserializer(deserializer, INT_TYPE));
        tmp.put(ShortType.class, new BasicTypeDeserializer(deserializer, SHORT_TYPE));
        tmp.put(LongType.class, new BasicTypeDeserializer(deserializer, LONG_TYPE));
        tmp.put(TimeType.class, new BasicTypeDeserializer(deserializer, LONG_TYPE));
        tmp.put(CounterColumnType.class, new BasicTypeDeserializer(deserializer, LONG_TYPE));
        tmp.put(SimpleDateType.class, new BasicTypeDeserializer(deserializer, DATE_TYPE));
        // Logical Types
        tmp.put(InetAddressType.class, new InetAddressDeserializer(deserializer));
        tmp.put(TimestampType.class, new TimestampTypeDeserializer(deserializer));
        tmp.put(DurationType.class, new DurationTypeDeserializer(deserializer));
        tmp.put(UUIDType.class, new UUIDTypeDeserializer(deserializer));
        tmp.put(TimeUUIDType.class, new TimeUUIDTypeDeserializer(deserializer));
        tmp.put(IntegerType.class, new VarIntTypeDeserializer(deserializer));
        // Collection Types
        tmp.put(ListType.class, new ListTypeDeserializer(deserializer));
        tmp.put(SetType.class, new SetTypeDeserializer(deserializer));
        tmp.put(MapType.class, new MapTypeDeserializer(deserializer));
        // Struct Types
        tmp.put(TupleType.class, new TupleTypeDeserializer());
        tmp.put(UserType.class, new UserDefinedTypeDeserializer());

        TYPE_MAP = Collections.unmodifiableMap(tmp);
    }

    /**
     * Deserialize from snapshot/datastax-sourced cassandra data.
     *
     * @param dataType the {@link DataType} of the object
     * @param bb       the bytes of the column to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(DataType dataType, ByteBuffer bb) {
        AbstractType abstractType = CassandraTypeConverter.convert(dataType);
        return deserialize(abstractType, bb);
    }

    /**
     * Deserialize from cdc-log-sourced cassandra data.
     *
     * @param abstractType the {@link AbstractType} of the non-collection column
     * @param bb           the bytes of the non-collection column to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        if (bb == null) {
            return null;
        }

        // Check if abstract type is reversed, if yes, use the base type for deserialization.
        if (abstractType.isReversed()) {
            abstractType = ((ReversedType) abstractType).baseType;
        }

        TypeDeserializer typeDeserializer = getTypeDeserializer(abstractType);
        return typeDeserializer.deserialize(abstractType, bb);
    }

    /**
     * Deserialize from cdc-log-sourced cassandra data.
     *
     * @param collectionType the {@link CollectionType} of the collection column
     * @param ccd            the ComplexColumnData of the collection column to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(CollectionType<?> collectionType, ComplexColumnData ccd) {
        TypeDeserializer typeDeserializer = getTypeDeserializer(collectionType);
        return ((CollectionTypeDeserializer) typeDeserializer).deserialize(collectionType, ccd);
    }

    /**
     * Construct a kafka connect SchemaBuilder object from a Cassandra data type
     *
     * @param abstractType implementation of Cassandra's AbstractType
     * @return the kafka connect SchemaBuilder object
     */
    public static SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        TypeDeserializer typeDeserializer = getTypeDeserializer(abstractType);
        return typeDeserializer.getSchemaBuilder(abstractType);
    }

    /**
     * Get TypeDeserializer of AbstractType
     *
     * @param abstractType the {@link AbstractType} of a column in cassandra
     * @return the TypeDeserializer of the AbstractType.
     */
    public static TypeDeserializer getTypeDeserializer(AbstractType<?> abstractType) {
        return CassandraTypeDeserializer.getInstance().TYPE_MAP.get(abstractType.getClass());
    }

    /**
     * Set deserialization mode for varint columns
     *
     * @param varIntHandlingMode the {@link VarIntHandlingMode} of varint values
     */
    public static void setVarIntHandlingMode(VarIntHandlingMode varIntHandlingMode) {
        VarIntTypeDeserializer varIntDeserializer = (VarIntTypeDeserializer) CassandraTypeDeserializer.getInstance().TYPE_MAP.get(IntegerType.class);
        varIntDeserializer.setMode(varIntHandlingMode);
    }
}
