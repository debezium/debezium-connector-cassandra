/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.shaded.fasterxml.jackson.databind.type.CollectionType;
import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.CollectionTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.DecimalTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.VarIntTypeDeserializer;

@ThreadSafe
@Immutable
public final class CassandraTypeDeserializer {

    private Map<Class<?>, TypeDeserializer> TYPE_MAP;

    private Map<Integer, TypeDeserializer> DATATYPE_MAP;

    private Function<Object, Object> baseType;

    public enum DecimalMode {
        PRECISE,
        DOUBLE,
        STRING
    }

    public enum VarIntMode {
        PRECISE,
        LONG,
        STRING
    }

    private CassandraTypeDeserializer() {
    }

    private static final class CassandraTypeDeserializerInstanceHolder {
        private static final CassandraTypeDeserializer instance = new CassandraTypeDeserializer();
    }

    public static CassandraTypeDeserializer getInstance() {
        return CassandraTypeDeserializerInstanceHolder.instance;
    }

    public static void init(List<AbstractTypeDeserializer> typeDeserializers, DecimalMode decimalMode, VarIntMode varIntMode, Function<Object, Object> baseType) {
        CassandraTypeDeserializer instance = getInstance();
        instance.initInternal(typeDeserializers, decimalMode, varIntMode, baseType);
    }

    private void initInternal(List<AbstractTypeDeserializer> typeDeserializers, DecimalMode decimalMode, VarIntMode varIntMode, Function<Object, Object> baseType) {
        if (TYPE_MAP != null) {
            return;
        }

        Map<Class<?>, TypeDeserializer> tmp = new HashMap<>();
        Map<Integer, TypeDeserializer> tmpDatatypeMap = new HashMap<>();
        typeDeserializers.forEach(typeDeserializer -> {
            tmp.put(typeDeserializer.getAbstractTypeClass(), typeDeserializer);
            tmpDatatypeMap.put(typeDeserializer.getDataType(), typeDeserializer);
        });

        TYPE_MAP = Collections.unmodifiableMap(tmp);
        DATATYPE_MAP = Collections.unmodifiableMap(tmpDatatypeMap);
        this.baseType = baseType;

        setDecimalMode(decimalMode);
        setVarIntMode(varIntMode);
    }

    private Object baseType(Object abstractType) {
        return baseType.apply(abstractType);
    }

    /**
     * Deserialize from snapshot/datastax-sourced cassandra data.
     *
     * @param dataType the {@link DataType} of the object
     * @param bb       the bytes of the column to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(DataType dataType, ByteBuffer bb) {
        if (bb == null) {
            return null;
        }

        TypeDeserializer typeDeserializer = getTypeDeserializer(dataType);
        return typeDeserializer.deserialize(typeDeserializer.getAbstractType(dataType), bb);
    }

    /**
     * Deserialize from cdc-log-sourced cassandra data.
     *
     * @param abstractType the AbstractType of the non-collection column
     * @param bb           the bytes of the non-collection column to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(Object abstractType, ByteBuffer bb) {
        if (bb == null) {
            return null;
        }

        abstractType = getInstance().baseType(abstractType);
        TypeDeserializer typeDeserializer = getTypeDeserializer(abstractType);
        return typeDeserializer.deserialize(abstractType, bb);
    }

    /**
     * Deserialize from cdc-log-sourced cassandra data.
     *
     * @param collectionType the {@link CollectionType} of the collection column
     * @param bbList         the ComplexColumnData of the collection column to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(Object collectionType, List<ByteBuffer> bbList) {
        TypeDeserializer typeDeserializer = getTypeDeserializer(collectionType);
        return ((CollectionTypeDeserializer) typeDeserializer).deserialize(collectionType, bbList);
    }

    /**
     * Construct a kafka connect SchemaBuilder object from a Cassandra data type
     *
     * @param abstractType implementation of Cassandra's AbstractType
     * @return the kafka connect SchemaBuilder object
     */
    public static SchemaBuilder getSchemaBuilder(Object abstractType) {
        TypeDeserializer typeDeserializer = getTypeDeserializer(abstractType);
        return typeDeserializer.getSchemaBuilder(abstractType);
    }

    public static SchemaBuilder getSchemaBuilder(DataType dataType) {
        TypeDeserializer typeDeserializer = getTypeDeserializer(dataType);
        return typeDeserializer.getSchemaBuilder(typeDeserializer.getAbstractType(dataType));
    }

    /**
     * Get TypeDeserializer of AbstractType
     *
     * @param abstractType the AbstractType of a column in cassandra
     * @return the TypeDeserializer of the AbstractType.
     */
    public static TypeDeserializer getTypeDeserializer(Object abstractType) {
        return CassandraTypeDeserializer.getInstance().TYPE_MAP.get(abstractType.getClass());
    }

    public static TypeDeserializer getTypeDeserializer(DataType dataType) {
        return CassandraTypeDeserializer.getInstance().DATATYPE_MAP.get(dataType.getProtocolCode());
    }

    /**
     * Set deserialization mode for decimal columns
     *
     * @param decimalMode the {@link DecimalMode} of decimal values
     */
    public static void setDecimalMode(DecimalMode decimalMode) {
        DecimalTypeDeserializer decimalDeserializer = (DecimalTypeDeserializer) CassandraTypeDeserializer
                .getInstance().DATATYPE_MAP.get(ProtocolConstants.DataType.DECIMAL);
        decimalDeserializer.setMode(decimalMode);
    }

    /**
     * Set deserialization mode for varint columns
     *
     * @param varIntMode the {@link VarIntMode} of varint values
     */
    public static void setVarIntMode(VarIntMode varIntMode) {
        VarIntTypeDeserializer varIntDeserializer = (VarIntTypeDeserializer) CassandraTypeDeserializer.getInstance().DATATYPE_MAP.get(ProtocolConstants.DataType.VARINT);
        varIntDeserializer.setMode(varIntMode);
    }
}
